package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	LeaderId int
	Seq      int
	ClientId int64
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
	// You'll have to add code here.
	ck.ClientId = nrand()
	ck.Seq = 1
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	return ck.CmdRequst(&CommandArgs{Key: key, Value: "", Op: "Get"})
	// You will have to modify this function.
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.CmdRequst(&CommandArgs{Key: key, Value: value, Op: op})
}

func (ck *Clerk) CmdRequst(args *CommandArgs) string {

	args.ClientId = ck.ClientId
	args.Seq = ck.Seq
	LeaderId := ck.LeaderId
	for {
		reply := CmdReply{}
		ok := ck.servers[LeaderId%len(ck.servers)].Call("KVServer.CommandReply", args, &reply)
		if ok {
			switch reply.Err {
			case OK:
				ck.LeaderId = LeaderId % len(ck.servers)
				ck.Seq++		
				return reply.Value
			case ErrNoKey:
				ck.LeaderId = LeaderId % len(ck.servers)
				ck.Seq++
				return ""
			}
		}
		LeaderId++
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
