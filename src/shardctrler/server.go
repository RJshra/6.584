package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead       int32    // set by Kill()
	configs    []Config // indexed by config num
	Client2Seq map[int64]int
	Index2Cmd  map[int]chan result
}

type Op struct {
	// Your data here.
	Command  string
	ClientId int64
	Seq      int
	Servers  map[int][]string //Join
	GIDs     []int            //leave
	Shard    int              //Move
	GID      int              //Move
	Num      int              //Query
}

type result struct {
	ClientId int64
	Seq      int
	Res      Config
}

func (sc *ShardCtrler) Join(args *CmdArgs, reply *CmdReply) {
	// Your code here.
	sc.Command(args, reply)
}

func (sc *ShardCtrler) Leave(args *CmdArgs, reply *CmdReply) {
	// Your code here.
	sc.Command(args, reply)
}

func (sc *ShardCtrler) Move(args *CmdArgs, reply *CmdReply) {
	// Your code here.
	sc.Command(args, reply)
}

func (sc *ShardCtrler) Query(args *CmdArgs, reply *CmdReply) {
	// Your code here.
	sc.Command(args, reply)
}

func (sc *ShardCtrler) GetChan(index int) chan result {

	ch, exist := sc.Index2Cmd[index]
	if !exist {
		ch = make(chan result, 1)
		sc.Index2Cmd[index] = ch
	}
	return ch
}

func (sc *ShardCtrler) apply() {
	for !sc.killed() {
		select {
		case ch := <-sc.applyCh:
			if ch.CommandValid {
				sc.mu.Lock()

				opchan := sc.GetChan(ch.CommandIndex)
				op := ch.Command.(Op)
				res := result{
					ClientId: op.ClientId,
					Seq:      op.Seq,
				}
				if sc.Client2Seq[op.ClientId] < op.Seq {

					switch op.Command {
					case Join:
						{
							sc.configs = append(sc.configs, *sc.JoinProcess(op.Servers))
							//fmt.Printf("Join %v\n", sc.configs[len(sc.configs)-1])
						}
					case Leave:
						{
							sc.configs = append(sc.configs, *sc.LeaveProcess(op.GIDs))
							//fmt.Printf("leave %v\n", sc.configs[len(sc.configs)-1])
						}
					case Move:
						{
							sc.configs = append(sc.configs, *sc.MoveProcess(op.GID, op.Shard))
							//fmt.Printf("move %v\n", sc.configs[len(sc.configs)-1])
						}
					case Query:
						{
							res.Res = sc.QueryProcess(op.Num)
							//fmt.Printf("query %v\n", res.Res)
						}
					}

					sc.Client2Seq[op.ClientId] = op.Seq
					sc.mu.Unlock()
					opchan <- res
				}else{
					
					sc.mu.Unlock()
				}

			}

		}
	}
}

func (sc *ShardCtrler) Command(args *CmdArgs, reply *CmdReply) {
	if sc.killed() {
		reply.Err = WrongLeader
		return
	}
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = WrongLeader
		return
	}
	sc.mu.Lock()
	if sc.Client2Seq[args.ClientId] >= args.Seq {
		reply.Err = OK
		if args.Op == Query {
			reply.Config = sc.QueryProcess(args.Num)

		}
		sc.mu.Unlock()
		return
	}

	op := Op{
		Command:  args.Op,
		Seq:      args.Seq,
		ClientId: args.ClientId,
		Servers:  args.Servers, //Join
		GIDs:     args.GIDs,    //leave
		Shard:    args.Shard,   //Move
		GID:      args.GID,     //Move
		Num:      args.Num,     //Query
	}

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = WrongLeader
		sc.mu.Unlock()
		return
	}
	ch := sc.GetChan(index)
	sc.mu.Unlock()
	
	select {
	case app := <-ch:
		if app.ClientId == op.ClientId && app.Seq == op.Seq {
			reply.Err = OK
			reply.Config = app.Res
		} else {

			reply.Err = WrongLeader
		

		}

	case <-time.After(time.Millisecond * 33):
		reply.Err = WrongLeader
	}

	go func() {
		sc.mu.Lock()
		delete(sc.Index2Cmd, index)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) GetLastConfig() Config {
	if len(sc.configs) > 0 {
		return sc.configs[len(sc.configs)-1]
	}
	return Config{}
}

func (sc *ShardCtrler) JoinProcess(Servers map[int][]string) *Config {
	lastConfig := sc.GetLastConfig()
	lastGroup := make(map[int][]string)
	for k, v := range lastConfig.Groups {
		lastGroup[k] = v
	}
	newConfig := Config{Num: lastConfig.Num + 1, Shards: lastConfig.Shards, Groups: lastGroup}
	for k, v := range Servers {
		newConfig.Groups[k] = v
	}
	if len(lastGroup) <= 10 {
		newConfig.Shards = sc.BalanceLoad(lastConfig.Shards, newConfig.Groups)
	}

	return &newConfig
}

func (sc *ShardCtrler) LeaveProcess(GIDs []int) *Config {
	lastConfig := sc.GetLastConfig()
	lastGroup := make(map[int][]string)
	for k, v := range lastConfig.Groups {
		lastGroup[k] = v
	}
	newConfig := Config{Num: lastConfig.Num + 1, Shards: lastConfig.Shards, Groups: lastGroup}
	for _, value := range GIDs {
		delete(newConfig.Groups, value)
	}
	newConfig.Shards = sc.BalanceLoad(lastConfig.Shards, newConfig.Groups)
	return &newConfig
}

func (sc *ShardCtrler) MoveProcess(gid int, shard int) *Config {
	lastConfig := sc.GetLastConfig()
	lastGroup := make(map[int][]string)
	for k, v := range lastConfig.Groups {
		lastGroup[k] = v
	}
	newConfig := Config{Num: lastConfig.Num + 1, Shards: lastConfig.Shards, Groups: lastGroup}
	newConfig.Shards[shard] = gid
	return &newConfig
}

func (sc *ShardCtrler) QueryProcess(num int) Config {
	if num == -1 || num >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	}

	return sc.configs[num]
}

type Pair struct {
	Key   int
	Value interface{}
}

type PairList []Pair

func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Key < p[j].Key }
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (sc *ShardCtrler) BalanceLoad(Shards [NShards]int, Group map[int][]string) [NShards]int {
	gid2shards := make(map[int][]int)
	flag := 0
	for k, v := range Shards {
		gid2shards[v] = append(gid2shards[v], k)
		if v == 0 {
			flag = 1
		}
	}

	var res [NShards]int
	if len(Group) == 0 {
		i := 0
		for i < NShards {
			res[i] = 0
			i++
		}
		return res
	}
	length := NShards / len(Group)
	remain := NShards % len(Group)
	lack := make(PairList, 0)
	gid2index := make(map[int]int)
	group := make(PairList, len(Group))
	index := 0
	for k, v := range Group {
		group[index] = Pair{k, v}
		index++
	}
	sort.Sort(group)

	oldGroup := sc.configs[len(sc.configs)-1].Groups

	//join
	if len(oldGroup) < len(Group) || flag == 1 {
		if len(gid2shards) == 0 || flag == 1 {
			count := 0
			for _, v := range group {
				i := 0
				for i < length {
					res[i+count*length] = v.Key
					i++
				}
				count++
			}
			j := 0

			for _, v := range group {
				if j >= remain {
					break
				}
				res[NShards-j-1] = v.Key
				j++

			}
			//fmt.Printf("first join %v\n", res)
			return res
		}
		ngid2shards := make(PairList, len(Group))
		sum := len(Group)
		newGID := 0
		//first:=0
		newGIDs := make(PairList, 0)
		for k := range Group {
			_, ok := gid2shards[k]
			if !ok {
				//newGID[k] = 1
				gid2shards[k] = make([]int, 0)
				tmp := Pair{k, ""}
				newGIDs = append(newGIDs, tmp)
			}
		}

		sort.Sort(newGIDs)
		newGID = newGIDs[0].Key
		i := 0
		for k, v := range gid2shards {
			ngid2shards[i] = Pair{k, v}
			i++
		}
		sort.Sort(ngid2shards)
		for k, v := range ngid2shards {
			gid2index[v.Key] = k
		}
		for i, value := range ngid2shards {
			gid, count := ngid2shards[i].Key, value.Value.([]int)
			if gid == newGID {
				sum--
				continue
			}
			if len(count) > length {
				sum--
				tmp := make([]int, 0)
				if sum >= remain {
					tmp = append(tmp, count[length:]...)
					ngid2shards[i].Value = ngid2shards[i].Value.([]int)[:length]
				} else {
					tmp = append(tmp, count[length+1:]...)
					ngid2shards[i].Value = ngid2shards[i].Value.([]int)[:length+1]
				}
				ngid2shards[gid2index[newGID]].Value = append(ngid2shards[gid2index[newGID]].Value.([]int), tmp...)
			} else if len(count) == length {
				sum--
				if sum < remain {
					lack = append(lack, Pair{gid, 1})
				}
				continue
			} else {
				sum--
				if sum >= remain {
					lack = append(lack, Pair{gid, length - len(count)})
					//lack[gid] = length - len(count)
				} else {
					//lack[gid] = length - len(count) + 1
					lack = append(lack, Pair{gid, length - len(count) + 1})
				}

			}
		}

		if len(lack) != 0 {
			sort.Sort(lack)
			for _, value := range lack {
				k, v := value.Key, value.Value.(int)
				tmp := ngid2shards[gid2index[newGID]].Value.([]int)[:v]
				ngid2shards[gid2index[newGID]].Value = ngid2shards[gid2index[newGID]].Value.([]int)[v:]
				ngid2shards[gid2index[k]].Value = append(ngid2shards[gid2index[k]].Value.([]int), tmp...)
			}
		}

		for _, v := range ngid2shards {
			k := v.Key
			for _, value := range v.Value.([]int) {
				res[value] = k
			}
		}
	} else {
		oldGID := 0
		remove := make(map[int]int)
		oldGIDs := make(PairList, 0)
		for k := range gid2shards {
			_, ok := Group[k]
			if !ok {
				remove[k] = 1
				tmp := Pair{k, ""}
				oldGIDs = append(oldGIDs, tmp)
			}
		}
		sort.Sort(oldGIDs)
		if len(oldGIDs) == 0 {
			return Shards
		}
		oldGID = oldGIDs[0].Key
		ngid2shards := make(PairList, len(gid2shards))
		sum := len(Group)
		i := 0
		for k, v := range gid2shards {
			ngid2shards[i] = Pair{k, v}
			i++
		}
		sort.Sort(ngid2shards)

		for k, v := range ngid2shards {
			gid2index[v.Key] = k
		}

		for _, value := range ngid2shards {
			gid, count := value.Key, value.Value.([]int)
			if gid == oldGID {
				continue
			}
			if _, ok := remove[gid]; ok {
				ngid2shards[gid2index[oldGID]].Value = append(ngid2shards[gid2index[oldGID]].Value.([]int), ngid2shards[gid2index[gid]].Value.([]int)...)
				tmp := make([]int, 0)
				ngid2shards[gid2index[gid]].Value = tmp
				continue
			}
			if len(count) < length {
				sum--
				if sum >= remain {
					if len(ngid2shards[gid2index[oldGID]].Value.([]int)) >= length-len(count) {
						tmp := ngid2shards[gid2index[oldGID]].Value.([]int)[:length-len(count)]
						ngid2shards[gid2index[oldGID]].Value = ngid2shards[gid2index[oldGID]].Value.([]int)[length-len(count):]
						ngid2shards[gid2index[gid]].Value = append(ngid2shards[gid2index[gid]].Value.([]int), tmp...)

					} else {
						//lack[gid] = length - len(count)
						lack = append(lack, Pair{gid, length - len(count)})
					}

				} else {
					if len(ngid2shards[gid2index[oldGID]].Value.([]int)) >= length-len(count)+1 {
						tmp := ngid2shards[gid2index[oldGID]].Value.([]int)[:length-len(count)+1]
						ngid2shards[gid2index[oldGID]].Value = ngid2shards[gid2index[oldGID]].Value.([]int)[length-len(count)+1:]
						ngid2shards[gid2index[gid]].Value = append(ngid2shards[gid2index[gid]].Value.([]int), tmp...)

					} else {
						//lack[gid] = length - len(count)+1
						lack = append(lack, Pair{gid, length - len(count) + 1})
					}
				}

			} else if len(count) == length {
				sum--
				continue
			} else {
				sum--
				if len(Group) >= NShards {
					continue
				}
				if sum >= remain {
					ngid2shards[gid2index[oldGID]].Value = append(ngid2shards[gid2index[oldGID]].Value.([]int), gid2shards[gid][length:]...)
					ngid2shards[gid2index[gid]].Value = gid2shards[gid][:length]
				} else {
					ngid2shards[gid2index[oldGID]].Value = append(ngid2shards[gid2index[oldGID]].Value.([]int), gid2shards[gid][length+1:]...)
				}

			}
		}
		if len(lack) != 0 {
			sort.Sort(lack)
			for _, value := range lack {
				k, v := value.Key, value.Value.(int)
				tmp := ngid2shards[gid2index[oldGID]].Value.([]int)[:v]
				ngid2shards[gid2index[oldGID]].Value = ngid2shards[gid2index[oldGID]].Value.([]int)[v:]
				ngid2shards[gid2index[k]].Value = append(ngid2shards[gid2index[k]].Value.([]int), tmp...)

			}
		}
		if len(ngid2shards[gid2index[oldGID]].Value.([]int)) > 0 && len(Group) >= NShards {
			count := ngid2shards[gid2index[oldGID]].Value.([]int)
			for _, v := range group {
				if len(count) == 0 {
					tmp := make([]int, 0)
					ngid2shards[gid2index[oldGID]].Value = tmp
					break
				}
				if _, ok := gid2shards[v.Key]; !ok {
					t := make([]int, 0)
					t = append(t, count[0])
					count = count[1:]
					tmp := Pair{v.Key, t}
					ngid2shards = append(ngid2shards, tmp)
				}
			}
		}
		//fmt.Printf("now value %v\n",ngid2shards[gid2index[oldGID]])
		for i, value := range ngid2shards {
			v := value.Value.([]int)
			if i == gid2index[oldGID] {
				continue
			}
			if len(ngid2shards[gid2index[oldGID]].Value.([]int)) == 0 {
				break
			}
			if len(v) == length {
				ngid2shards[i].Value = append(v, ngid2shards[gid2index[oldGID]].Value.([]int)[:1]...)
				ngid2shards[gid2index[oldGID]].Value = ngid2shards[gid2index[oldGID]].Value.([]int)[1:]
			}
		}
		for _, v := range ngid2shards {
			k := v.Key
			for _, value := range v.Value.([]int) {
				res[value] = k
			}
		}
	}
	return res
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.Client2Seq = make(map[int64]int)
	sc.Index2Cmd = make(map[int]chan result)

	go sc.apply()
	return sc
}
