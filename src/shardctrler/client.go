package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	ClientId int64
	Seq      int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.ClientId = nrand()

	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &CmdArgs{}
	// Your code here.
	args.Num = num
	args.Op = Query
	args.ClientId = ck.ClientId
	ck.Seq++
	args.Seq = ck.Seq
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply CmdReply

			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.Err != WrongLeader {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &CmdArgs{}
	// Your code here.
	args.Servers = servers
	args.Op = Join
	args.ClientId = ck.ClientId
	ck.Seq++
	args.Seq = ck.Seq
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply CmdReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.Err != WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &CmdArgs{}
	// Your code here.
	args.GIDs = gids
	args.Op = Leave
	args.ClientId = ck.ClientId
	ck.Seq++
	args.Seq = ck.Seq
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply CmdReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.Err != WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &CmdArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.Op = Move
	args.ClientId = ck.ClientId
	ck.Seq++
	args.Seq = ck.Seq
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply CmdReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.Err != WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
