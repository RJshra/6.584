package shardkv

import (
	"bytes"
	"encoding/gob"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

// type Op struct {
// 	// Your definitions here.
// 	// Field names must start with capital letters,
// 	// otherwise RPC will break.
// 	Key     string
// 	Value   string
// 	Command int

// 	Seq      int
// 	ClientId int64

// 	DB         map[string]string
// 	Client2Seq map[int64]int
// 	Servers    []string
// 	ShardId    int
// 	ShardValid bool
// 	Num        int
// 	Config     shardctrler.Config
// }

type Op struct {
	ShardValid bool
	Cmd        interface{}
}

type KVOp struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Command  int
	Seq      int
	ClientId int64
}

type ShardOp struct {
	Command    int
	DB         map[string]string
	Client2Seq map[int64]int
	Servers    []string
	ShardId    int
	Num        int
	Config     shardctrler.Config
}

type outedData struct {
	DB      KV
	ShardId int
}

type PullArgs struct {
	Num     int
	ShardId int
}

const (
	Not  = "Not"
	Pull = "Pull"
	// MigrateShard = "Migrate"
	// RemoveShard  = "Remove"

	// Finish        = "Finish"
	// Configuration = "Conf"
	// Clearshard    = "Clear"
	MigrateShard = 1
	RemoveShard  = 2

	Finish        = 3
	Configuration = 4
	Clearshard    = 5
	PUT           = 6
	APPEND        = 7
	GET           = 8
	PULL          = 9
)

type PullReply struct {
	Err        Err
	DB         map[string]string
	Client2Seq map[int64]int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int   // snapshot if log grows this big
	dead         int32 // set by Kill()
	// Your definitions here.
	LastApplied  int
	StateMachine KVStateMachine
	Client2Seq   map[int64]int
	Index2Cmd    map[int]chan KVOp

	sm           *shardctrler.Clerk
	LastConfig   shardctrler.Config
	PreConfig    shardctrler.Config
	ShardState   map[int]string
	ShardNum     map[int]int
	OutedData    map[int]map[int]map[string]string //num->shard id->data
	Shard2Client map[int][]int64

	Pullchan map[int]chan PullReply
}

func (kv *ShardKV) GetChan(index int) chan KVOp {

	ch, exist := kv.Index2Cmd[index]
	if !exist {
		ch = make(chan KVOp, 1)
		kv.Index2Cmd[index] = ch
	}
	return ch
}

type KVStateMachine interface {
	Get(key string) (string, Err)
	Put(key string, value string) Err
	Append(key string, value string) Err
	Migrate(shardId int, shard map[string]string) Err
	Copy(shardId int) map[string]string
	Remove(shardId int) Err
	
}

type KV struct {
	K2V map[int]map[string]string
}

func (kv *KV) Get(key string) (string, Err) {
	value, ok := kv.K2V[key2shard(key)][key]
	if ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (kv *KV) Put(key string, value string) Err {
	if len(kv.K2V[key2shard(key)]) == 0 {
		kv.K2V[key2shard(key)] = make(map[string]string)
	}
	kv.K2V[key2shard(key)][key] = value
	return OK
}

func (kv *KV) Append(key string, value string) Err {
	kv.K2V[key2shard(key)][key] += value
	return OK
}

func (kv *KV) Migrate(shardId int, shard map[string]string) Err {
	delete(kv.K2V, shardId)
	kv.K2V[shardId] = make(map[string]string)
	for k, v := range shard {
		kv.K2V[shardId][k] = v
	}
	return OK
}

func (kv *KV) Copy(shardId int) map[string]string {
	res := make(map[string]string)
	for k, v := range kv.K2V[shardId] {
		res[k] = v
	}
	return res
}

func (kv *KV) Remove(shardId int) Err {
	delete(kv.K2V, shardId)
	return OK
}


func (kv *ShardKV) Get(args *CommandArgs, reply *CommandReply) {
	// Your code here.
	kv.Command(args, reply)
}

func (kv *ShardKV) applyStateMachine(op *KVOp) {
	switch op.Command {
	case PUT:
		kv.StateMachine.Put(op.Key, op.Value)
	case APPEND:
		kv.StateMachine.Append(op.Key, op.Value)
	}
}

func (kv *ShardKV) PutAppend(args *CommandArgs, reply *CommandReply) {
	// Your code here.
	kv.Command(args, reply)
}

func (kv *ShardKV) CheckGroup(key string) bool {
	return kv.PreConfig.Shards[key2shard(key)] == kv.gid
}

func (kv *ShardKV) Command(args *CommandArgs, reply *CommandReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()

	if !kv.CheckGroup(args.Key) || kv.ShardState[key2shard(args.Key)] != OK {
		reply.Err = ErrWrongGroup
		
		kv.mu.Unlock()
		return
	}
	
	if args.Op != "Get" && kv.Client2Seq[args.ClientId] >= args.Seq {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	intcmd := 0
	switch args.Op {
	case Put:
		intcmd = PUT
	case Append:
		intcmd = APPEND
	case Get:
		intcmd = GET
	}
	op := KVOp{
		Key:      args.Key,
		Value:    args.Value,
		Command:  intcmd,
		Seq:      args.Seq,
		ClientId: args.ClientId,
	}
	index, _, isLeader := kv.rf.Start(Op{ShardValid: false, Cmd: op})
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	ch := kv.GetChan(index)
	kv.mu.Unlock()
	select {
	case app := <-ch:
		if app.ClientId == op.ClientId && app.Seq == op.Seq {
			if args.Op == "Get" {

				reply.Value = app.Value
				if reply.Value != "" {
					reply.Err = OK
				} else {
					reply.Err = ErrNoKey
				}

			} else {
				reply.Err = OK

			}

		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(time.Millisecond * 200):
		reply.Err = ErrWrongLeader
	}

	go func() {
		kv.mu.Lock()
		delete(kv.Index2Cmd, index)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKV) apply() {
	for !kv.killed() {
		select {
		case ch := <-kv.applyCh:
			if ch.CommandValid {
				kv.mu.Lock()
				tmp := ch.Command.(Op)
				if tmp.ShardValid {
					op := tmp.Cmd.(ShardOp)
					switch op.Command {
					case MigrateShard:
						{

							if kv.ShardNum[op.ShardId] < op.Num && kv.ShardState[op.ShardId] != OK {
								kv.StateMachine.Migrate(op.ShardId, op.DB)
								delete(kv.Shard2Client, op.ShardId)
								for k, v := range op.Client2Seq {
									if kv.Client2Seq[k] < v {
										kv.Client2Seq[k] = v
									}
							
									if len(kv.Shard2Client[op.ShardId]) == 0 {
										kv.Shard2Client[op.ShardId] = append(kv.Shard2Client[op.ShardId], k)
									} else {
										flag := true
										for _, exitClient := range kv.Shard2Client[op.ShardId] {
											if exitClient == k {
												flag = false
												break
											}
										}
										if flag {
											kv.Shard2Client[op.ShardId] = append(kv.Shard2Client[op.ShardId], k)
										}
									}

								}
								kv.ShardState[op.ShardId] = OK
								kv.ShardNum[op.ShardId] = op.Num
								args := PullArgs{Num: op.Num - 1, ShardId: op.ShardId}
								kv.SendClearShard(op.Servers, &args)

							}

						}

					case Configuration:
						{

							if kv.PreConfig.Num+1 == op.Config.Num {
								kv.LastConfig = kv.PreConfig
								kv.PreConfig = op.Config
								for shardid, gid := range kv.PreConfig.Shards {
									if gid != kv.gid && kv.LastConfig.Shards[shardid] == kv.gid {
										if kv.ShardState[shardid] == OK {
											CloneMap := kv.StateMachine.Copy(shardid)
											if len(CloneMap) == 0 {
												delete(kv.OutedData[op.Config.Num-1], shardid)
											} else {

												if len(kv.OutedData[op.Config.Num-1]) == 0 {
													tmp := make(map[int]map[string]string)
													tmp[shardid] = CloneMap
													kv.OutedData[op.Config.Num-1] = tmp
												} else {
													kv.OutedData[op.Config.Num-1][shardid] = CloneMap
												}

											}

											kv.StateMachine.Remove(shardid)
											kv.ShardState[shardid] = Not
											kv.ShardNum[shardid] = op.Config.Num
										}
									} else if gid == kv.gid && kv.LastConfig.Shards[shardid] == kv.gid {
										kv.ShardNum[shardid] = op.Config.Num
									}
								}
							}
						}
					case Clearshard:
						{
							delete(kv.OutedData[op.Num], op.ShardId)
							delete(kv.Shard2Client, op.ShardId)
							if len(kv.OutedData[op.Num]) == 0 {
								delete(kv.OutedData, op.Num)
							}

						}
					case PULL:
						{
							var reply PullReply
							tmp, ok := kv.ShardState[op.ShardId]
							if !ok || kv.ShardNum[op.ShardId] < op.Num {
								reply.Err = Not
							}
							if tmp == OK && kv.ShardNum[op.ShardId] == op.Num {

								reply.DB = kv.StateMachine.Copy(op.ShardId)
								CloneMap := kv.StateMachine.Copy(op.ShardId)
								if len(CloneMap) == 0 {
									delete(kv.OutedData[op.Num], op.ShardId)
								} else {

									if len(kv.OutedData[op.Num]) == 0 {
										tmp := make(map[int]map[string]string)
										tmp[op.ShardId] = CloneMap
										kv.OutedData[op.Num] = tmp
									} else {
										kv.OutedData[op.Num][op.ShardId] = CloneMap
									}

								}
								kv.StateMachine.Remove(op.ShardId)
								kv.ShardState[op.ShardId] = Not
								kv.ShardNum[op.ShardId] = op.Num
								
							} else {
								reply.DB = make(map[string]string)
								for k, v := range kv.OutedData[op.Num][op.ShardId] {
									reply.DB[k] = v
								}

							}
							reply.Err = OK
							reply.Client2Seq = make(map[int64]int)
							for k, v := range kv.Client2Seq {
								for _, value := range kv.Shard2Client[op.ShardId] {
									if k == value {
										reply.Client2Seq[k] = v
										break
									}
								}
							}
							ch2, exist := kv.Pullchan[op.Num*100+op.ShardId]
							if !exist {
								ch2 = make(chan PullReply, 1)
								kv.Pullchan[op.Num*100+op.ShardId] = ch2
							}
							go func() {
								select {
								case ch2 <- reply:
									return
								case <-time.After(time.Millisecond * 1000):
									return
								}

							}()
						}
					}
					if kv.LastApplied < ch.CommandIndex {
						kv.LastApplied = ch.CommandIndex
					}
					if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
						kv.rf.Snapshot(kv.LastApplied, kv.PersistSnapShot())

					}
					kv.mu.Unlock()
					continue
				}
				op := tmp.Cmd.(KVOp)
				if !kv.CheckGroup(op.Key) || kv.ShardState[key2shard(op.Key)] != OK {
					kv.mu.Unlock()
					continue
				}
				if ch.CommandIndex <= kv.LastApplied {
					kv.mu.Unlock()
					continue
				}

				kv.LastApplied = ch.CommandIndex
				opchan := kv.GetChan(ch.CommandIndex)
				
				if kv.Client2Seq[op.ClientId] < op.Seq {
					
					kv.applyStateMachine(&op)
					kv.Client2Seq[op.ClientId] = op.Seq
					if len(kv.Shard2Client[key2shard(op.Key)]) == 0 {
						kv.Shard2Client[key2shard(op.Key)] = make([]int64, 0)
					}
					flag := 0
					for _, v := range kv.Shard2Client[key2shard(op.Key)] {
						if v == op.ClientId {
							flag = 1
							break
						}
					}
					if flag == 0 {
						kv.Shard2Client[key2shard(op.Key)] = append(kv.Shard2Client[key2shard(op.Key)], op.ClientId)
					}

				}

				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
					kv.rf.Snapshot(ch.CommandIndex, kv.PersistSnapShot())

				}
				if op.Command == GET {
					op.Value, _ = kv.StateMachine.Get(op.Key)
				}

				kv.mu.Unlock()
				opchan <- op
			}

			if ch.SnapshotValid {
				kv.mu.Lock()
				if ch.SnapshotIndex > kv.LastApplied {

					kv.DecodeSnapShot(ch.Snapshot)

					kv.LastApplied = ch.SnapshotIndex
				}
				kv.mu.Unlock()
			}

		}
	}

}

func (kv *ShardKV) ClearShard(args *PullArgs, reply *PullReply) {
	kv.mu.Lock()
	delete(kv.OutedData[args.Num], args.ShardId)
	delete(kv.Shard2Client, args.ShardId)
	if len(kv.OutedData[args.Num]) == 0 {
		delete(kv.OutedData, args.Num)
	}
	_, _, isLeader := kv.rf.Start(Op{Cmd: ShardOp{Command: Clearshard, ShardId: args.ShardId, Num: args.Num}, ShardValid: true})

	if isLeader {
		reply.Err = OK
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) PullShard(args *PullArgs, reply *PullReply) {
	kv.mu.Lock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	if args.Num > kv.PreConfig.Num {
		reply.Err = Not
		kv.mu.Unlock()
		return
	}

	tmp, ok := kv.ShardState[args.ShardId]
	if !ok || kv.ShardNum[args.ShardId] < args.Num {
		reply.Err = Not
		kv.mu.Unlock()
		return
	}
	if tmp == OK && kv.ShardNum[args.ShardId] == args.Num {
		_, _, isleader := kv.rf.Start(Op{ShardValid: true, Cmd: ShardOp{Command: PULL, ShardId: args.ShardId, Num: args.Num}})
		if !isleader {
			reply.Err = Not
			kv.mu.Unlock()
			return
		}
		ch, exist := kv.Pullchan[args.Num*100+args.ShardId]
		if !exist {
			ch = make(chan PullReply, 1)
			kv.Pullchan[args.Num*100+args.ShardId] = ch
		}
		kv.mu.Unlock()
		select {
		case app := <-ch:
			reply.DB = app.DB
			reply.Err = OK
			reply.Client2Seq = app.Client2Seq

		case <-time.After(time.Millisecond * 1000):
			reply.Err = Not

		}

		go func() {
			kv.mu.Lock()
			delete(kv.Pullchan, args.Num*100+args.ShardId)
			kv.mu.Unlock()
		}()
		return
	
	} else {
		reply.DB = make(map[string]string)
		for k, v := range kv.OutedData[args.Num][args.ShardId] {
			reply.DB[k] = v
		}
	}
	reply.Err = OK
	reply.Client2Seq = make(map[int64]int)
	for k, v := range kv.Client2Seq {
		for _, value := range kv.Shard2Client[args.ShardId] {
			if k == value {
				reply.Client2Seq[k] = v
				break
			}
		}
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) SendClearShard(servers []string, args *PullArgs) {
	for si := 0; si < len(servers); si++ {
		srv := kv.make_end(servers[si])
		go func() {

			for !kv.killed() {
				var res PullReply
				ok := srv.Call("ShardKV.ClearShard", args, &res)
				if ok {
					return
				}

			}

		}()
	}
}

func (kv *ShardKV) SendPullShard(Group map[int][]string, args *PullArgs, oldgid int) {
	for !kv.killed() {
		if servers, ok := Group[oldgid]; ok {
			// try each server for the shard.
			var res PullReply
			flag := false
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				for !kv.killed() {
					var reply PullReply
					ok := srv.Call("ShardKV.PullShard", args, &reply)
					if ok {
						if reply.Err == ErrWrongLeader {
							break
						}
						if reply.Err == OK {
							if len(reply.DB) >= len(res.DB) {
								res.DB = reply.DB
								res.Client2Seq = reply.Client2Seq
								flag = true
							}
							break
						}
						if reply.Err == Not {
							break
						}
					} else {
						break
					}
					time.Sleep(time.Millisecond * 50)
				}
			}
			if flag {

				kv.mu.Lock()
				
				op := ShardOp{
					Command:    MigrateShard,
					DB:         res.DB,
					ShardId:    args.ShardId,
					Num:        args.Num + 1,
					Client2Seq: res.Client2Seq,
					Servers:    servers,
				}
				kv.rf.Start(Op{ShardValid: true, Cmd: op})
				kv.mu.Unlock()

				return
			}

		} else {
			return
		}
	}

}

func (kv *ShardKV) UpdateConfig() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		kv.mu.Lock()
		if kv.PreConfig.Num != kv.LastConfig.Num {
			canConfig := true
			for shardid, gid := range kv.PreConfig.Shards {
				if gid == kv.gid && kv.LastConfig.Shards[shardid] != kv.gid && kv.ShardNum[shardid] != kv.PreConfig.Num {
					args := PullArgs{Num: kv.PreConfig.Num - 1, ShardId: shardid}
					oldgid := kv.LastConfig.Shards[shardid]
					canConfig = false
					if oldgid == 0 {
						op := ShardOp{
							Command: MigrateShard,
							Num:     kv.PreConfig.Num,
							DB:      make(map[string]string),
							ShardId: shardid,
						}
						kv.rf.Start(Op{ShardValid: true, Cmd: op})

						continue
					}
					Group := make(map[int][]string)
					for k, v := range kv.LastConfig.Groups {
						Group[k] = v
					}
					go kv.SendPullShard(Group, &args, oldgid)
				}
			}
			if canConfig {
				kv.LastConfig = kv.PreConfig
			} 
			kv.mu.Unlock()
			time.Sleep(time.Millisecond * 33)
			continue
		}
		nextnum := kv.PreConfig.Num + 1
		kv.mu.Unlock()
		newConfig := kv.sm.Query(nextnum)
		kv.mu.Lock()
		if newConfig.Num == nextnum {
			//kv.rf.Start(Op{Command: Configuration, Config: newConfig, ShardValid: true})
			kv.rf.Start(Op{Cmd: ShardOp{Command: Configuration, Config: newConfig}, ShardValid: true})
			for shardid, gid := range newConfig.Shards {
				//lack
				if gid == kv.gid && kv.PreConfig.Shards[shardid] != kv.gid {
					oldgid := kv.PreConfig.Shards[shardid]
					if oldgid == 0 {
						op := ShardOp{
							Command: MigrateShard,
							Num:     kv.PreConfig.Num + 1,
							DB:      make(map[string]string),
							ShardId: shardid,
						}
						kv.rf.Start(Op{ShardValid: true, Cmd: op})

						continue
					}
					args := PullArgs{Num: nextnum - 1, ShardId: shardid}
					Group := make(map[int][]string)
					for k, v := range kv.PreConfig.Groups {
						Group[k] = v
					}
					go kv.SendPullShard(Group, &args, oldgid)

					
				}
				

			}

		}
		kv.mu.Unlock()
		time.Sleep(time.Millisecond * 33)
	}

}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()

}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var db KV

	var Client2Seq map[int64]int
	var preconfig shardctrler.Config
	var lastconfig shardctrler.Config
	var ShardState map[int]string
	var ShardNum map[int]int
	var OutedData map[int]map[int]map[string]string
	var Shard2Client map[int][]int64

	if d.Decode(&db) != nil ||
		d.Decode(&Client2Seq) != nil || d.Decode(&preconfig) != nil ||
		d.Decode(&lastconfig) != nil ||
		d.Decode(&ShardState) != nil ||
		d.Decode(&ShardNum) != nil ||
		d.Decode(&OutedData) != nil ||
		d.Decode(&Shard2Client) != nil {
	} else {
		kv.StateMachine = &db

		kv.Client2Seq = Client2Seq
		kv.PreConfig = preconfig
		kv.LastConfig = lastconfig
		kv.ShardState = ShardState
		kv.ShardNum = ShardNum
		kv.OutedData = OutedData
		kv.Shard2Client = Shard2Client
	
	}

}

func (kv *ShardKV) PersistSnapShot() []byte {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.StateMachine)
	e.Encode(kv.Client2Seq)
	e.Encode(kv.PreConfig)
	e.Encode(kv.LastConfig)
	e.Encode(kv.ShardState)
	e.Encode(kv.ShardNum)
	e.Encode(kv.OutedData)
	e.Encode(kv.Shard2Client)
	snapshot := w.Bytes()
	return snapshot
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.sm = shardctrler.MakeClerk(ctrlers)
	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.Client2Seq = make(map[int64]int)
	kv.Index2Cmd = make(map[int]chan KVOp)

	kv.StateMachine = &KV{make(map[int]map[string]string)}
	kv.OutedData = make(map[int]map[int]map[string]string)
	kv.LastConfig = shardctrler.Config{Num: 0, Groups: map[int][]string{}}
	kv.PreConfig = shardctrler.Config{Num: 0, Groups: map[int][]string{}}
	kv.ShardState = make(map[int]string)
	kv.ShardNum = make(map[int]int)
	kv.Shard2Client = make(map[int][]int64)
	kv.Pullchan = make(map[int]chan PullReply)
	snapshot := persister.ReadSnapshot()

	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}

	go kv.apply()
	go kv.UpdateConfig()
	gob.Register(ShardOp{})
	gob.Register(KVOp{})
	return kv
}
