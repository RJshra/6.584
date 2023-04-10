package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	TimeOut = "TimeOut"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type CommandArgs struct{
	Key   string
	Value string
	Op    string
	Seq int
	ClientId int64
}

type CmdReply struct{
	Err   Err
	Value string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Seq int
	ClientId int64
}

type GetReply struct {
	Err   Err
	Value string
}
