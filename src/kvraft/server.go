package kvraft

import (
	"bytes"
	
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command  string
	ClientId int64
	Seq      int
	Key      string
	Value    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	LastApplied  int
	StateMachine KVStateMachine
	Client2Seq   map[int64]int
	Index2Cmd    map[int]chan Op
}

type KVStateMachine interface {
	Get(key string) (string, Err)
	Put(key string, value string) Err
	Append(key string, value string) Err
}

type KV struct {
	K2V map[string]string
}

func (kv *KV) Get(key string) (string, Err) {
	value, ok := kv.K2V[key]
	if ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (KV *KV) Put(key string, value string) Err {
	KV.K2V[key] = value

	return OK
}

func (KV *KV) Append(key string, value string) Err {
	KV.K2V[key] += value
	return OK
}

func (kv *KVServer) applyStateMachine(op *Op) {
	switch op.Command {
	case "Put":
	
		kv.StateMachine.Put(op.Key, op.Value)
	case "Append":
	
		kv.StateMachine.Append(op.Key, op.Value)
	}
}

func (kv *KVServer) GetChan(index int) chan Op {

	ch, exist := kv.Index2Cmd[index]
	if !exist {
		ch = make(chan Op, 1)
		kv.Index2Cmd[index] = ch
	}
	return ch
}

func (kv *KVServer) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var StateMachine KV
	//var Index2Cmd map[int] chan Op
	var Client2Seq map[int64]int

	if d.Decode(&StateMachine) != nil ||
		d.Decode(&Client2Seq) != nil {
	} else {
		kv.StateMachine = &StateMachine
		kv.Client2Seq = Client2Seq
	}

}

func (kv *KVServer) PersistSnapShot() []byte {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.StateMachine)
	e.Encode(kv.Client2Seq)
	snapshot := w.Bytes()
	return snapshot
}

func (kv *KVServer) apply() {
	for !kv.killed() {
		select{
		case ch := <-kv.applyCh:
		if ch.CommandValid {
			kv.mu.Lock()
			if ch.CommandIndex <= kv.LastApplied {
				kv.mu.Unlock()
				continue
			}
		
			kv.LastApplied = ch.CommandIndex
			opchan := kv.GetChan(ch.CommandIndex)
			op := ch.Command.(Op)

			if kv.Client2Seq[op.ClientId] < op.Seq {
			
				kv.applyStateMachine(&op)
			
				kv.Client2Seq[op.ClientId] = op.Seq
			}

			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
				kv.rf.Snapshot(ch.CommandIndex, kv.PersistSnapShot())
			
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

func (kv *KVServer) CommandReply(args *CommandArgs, reply *CmdReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()

	if args.Op != "Get" && kv.Client2Seq[args.ClientId] >= args.Seq {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	//t := time.Time{}
	//t = time.Now()
	op := Op{
		Key:      args.Key,
		Value:    args.Value,
		Command:  args.Op,
		Seq:      args.Seq,
		ClientId: args.ClientId,
	}
	index, _, isLeader := kv.rf.Start(op)
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
				kv.mu.Lock()
				reply.Value, reply.Err = kv.StateMachine.Get(app.Key)
				kv.mu.Unlock()
			}
	
			reply.Err = OK
		} else {
			reply.Err = TimeOut
		}

	case <-time.After(time.Millisecond * 33):
		reply.Err = TimeOut
	}

	go func() {
		kv.mu.Lock()
		delete(kv.Index2Cmd, index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.Client2Seq = make(map[int64]int)
	kv.Index2Cmd = make(map[int]chan Op)
	kv.StateMachine = &KV{make(map[string]string)}

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}

	go kv.apply()
	// You may need initialization code here.

	return kv
}
