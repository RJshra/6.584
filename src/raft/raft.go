package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	Time     time.Time
	Term     int
	State    int
	VotedFor int
	//Count       int //store the counts of votes
	Logs        []Log
	CommitIndex int
	LastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCh     chan ApplyMsg
	Lastindex   int
	LastTerm    int
}

type Log struct {
	Command interface{}
	Term    int
	//Index   int
}

const Leader = 1
const Follower = 0
const Candidate = 2

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.Term
	isleader = rf.State == Leader
	return term, isleader
}

type InstallSnapshotRPC struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	//offset           int
	Data []byte
}
type InstallSnapshotReply struct {
	Term int
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Term)
	e.Encode(rf.Logs)
	e.Encode(rf.Lastindex)
	e.Encode(rf.LastTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var xxx int   //Votefor
	var yyy int   //term
	var zzz []Log //Logs
	var index int //term
	var term int  //term
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if d.Decode(&xxx) != nil ||
		d.Decode(&yyy) != nil || d.Decode(&zzz) != nil || d.Decode(&index) != nil || d.Decode(&term) != nil {
	} else {
		rf.VotedFor = xxx
		rf.Term = yyy
		rf.Logs = zzz
		rf.Lastindex = index
		rf.LastTerm = term
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.Lastindex || index > rf.CommitIndex {
		return
	}
	count := 1
	oldIndex := rf.Lastindex
	for key, value := range rf.Logs {

		if key == 0 {
			continue
		}
		count++
		rf.Lastindex = key + oldIndex

		rf.LastTerm = value.Term
		if key+oldIndex == index {
			break
		}

	}

	newLog := make([]Log, 1)
	newLog = append(newLog, rf.Logs[count:]...)
	rf.Logs = newLog

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Term)
	e.Encode(rf.Logs)
	e.Encode(rf.Lastindex)
	e.Encode(rf.LastTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

func (rf *Raft) InstallSnapShot(args *InstallSnapshotRPC, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.Term
	if args.Term < rf.Term {
	
		return
	}

	if args.Term > rf.Term {
		rf.Term = args.Term
		rf.VotedFor = -1
		rf.State = Follower
		rf.persist()
	}

	if args.LastIncludeIndex <= rf.Lastindex {
		return
	}
	rf.Time = time.Now()
	tmpLog := make([]Log, 1)
	for i := args.LastIncludeIndex + 1; i < rf.getLastIndex(); i++ {
		tmpLog = append(tmpLog, rf.Logs[i-rf.Lastindex])
	}
	rf.Lastindex = args.LastIncludeIndex
	rf.LastTerm = args.LastIncludeTerm

	rf.Logs = tmpLog

	if args.LastIncludeIndex > rf.CommitIndex {
		rf.CommitIndex = args.LastIncludeIndex
	}
	if args.LastIncludeIndex > rf.LastApplied {
		rf.LastApplied = args.LastIncludeIndex
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Term)
	e.Encode(rf.Logs)
	e.Encode(rf.Lastindex)
	e.Encode(rf.LastTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, args.Data)
	msg := ApplyMsg{
		Snapshot:      args.Data,
		SnapshotValid: true,
		SnapshotTerm:  rf.LastTerm,
		SnapshotIndex: rf.Lastindex,
	}
	go func() { rf.applyCh <- msg }()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type AppendEntriesRPC struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []Log
}

type AppendRes struct {
	Term    int
	Success bool
	Index   int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.Logs) == 1 {
		return rf.LastTerm
	} else {
		return rf.Logs[len(rf.Logs)-1].Term
	}
}

func (rf *Raft) getLastIndex() int {
	return rf.Lastindex + len(rf.Logs) - 1
}

func (rf *Raft) getLogTerm(index int) int {
	if index > rf.Lastindex {
		return rf.Logs[index-rf.Lastindex].Term
	}
	return rf.LastTerm
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.Term {
		reply.VoteGranted = false
		reply.Term = rf.Term
		return
	}
	if args.LastLogTerm < rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastIndex()) {
		reply.VoteGranted = false
		reply.Term = rf.Term
		if args.Term > rf.Term {
			rf.Term = args.Term
			rf.State = Follower
			rf.persist()
		}
		return
	}
	if args.Term > rf.Term || (args.Term == rf.Term && (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId)) {
		rf.State = Follower
		rf.VotedFor = args.CandidateId
		rf.Term = args.Term
		reply.VoteGranted = true
		reply.Term = rf.Term
		rf.Time = time.Now()
		rf.persist()
	}

}

func (rf *Raft) SyncLog(server int, args *AppendEntriesRPC, reply *AppendRes) {
	for {

		ok := rf.peers[server].Call("Raft.HeartBeat", args, reply)
		
		if !ok {
			return
		}
		rf.mu.Lock()
		if rf.State != Leader {
		
			rf.mu.Unlock()
			return
		}
		if !reply.Success {
		
			if reply.Term > rf.Term {
				rf.State = Follower
				rf.Term = reply.Term
				rf.VotedFor = -1
				rf.persist()
				rf.mu.Unlock()
				return
			} else {
				args.PrevLogIndex = reply.Index
				if args.PrevLogIndex < 0 {
					
					rf.mu.Unlock()
					return
				}

			
				if args.PrevLogIndex-rf.Lastindex < 0 {
					x := make([]byte, len(rf.persister.snapshot))
					copy(x, rf.persister.snapshot)
					margs := InstallSnapshotRPC{
						Term:             rf.Term,
						LeaderId:         rf.me,
						LastIncludeIndex: rf.Lastindex,
						LastIncludeTerm:  rf.LastTerm,
						Data:             x,
					}
			
					mreply := InstallSnapshotReply{}
					go rf.CallInstallsnapshop(server, &margs, &mreply)
					rf.mu.Unlock()
					return
				} else {
				
					args.PrevLogTerm = rf.getLogTerm(args.PrevLogIndex)
					entry := make([]Log, rf.getLastIndex()-args.PrevLogIndex)
					copy(entry, rf.Logs[args.PrevLogIndex-rf.Lastindex+1:])
					args.Entries = entry

				}

			

			}
			rf.mu.Unlock()
		} else {
			if rf.matchIndex[server] < args.PrevLogIndex+len(args.Entries) {
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.UpdateCommit()
			}
			if rf.nextIndex[server] < args.PrevLogIndex+len(args.Entries)+1 {
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			}
			
			rf.mu.Unlock()
			return
		}
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, count *int, res *int) bool {

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	*count = *count + 1
	if reply.VoteGranted {
		*res = *res + 1
	} else {
		if reply.Term > rf.Term {
			rf.State = Follower
			rf.Term = reply.Term
			rf.VotedFor = -1
			rf.Time = time.Now()
			rf.persist()
		}
	}
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.Term
	if rf.State != Leader {
		return index, term, false
	}
	newLog := Log{
		Command: command,
		Term:    rf.Term,
		//Index:   rf.getLastIndex() + 1,
	}
	rf.Logs = append(rf.Logs, newLog)
	rf.persist()
	rf.matchIndex[rf.me] = len(rf.Logs) - 1 + rf.Lastindex
	rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1
	i := 0
	for i < len(rf.peers) {
		if i == rf.me {
			i++
			continue
		}
		if rf.matchIndex[i] < rf.Lastindex {
			//do nothing
		} else {
			entry := make([]Log, rf.getLastIndex()-rf.matchIndex[i])

			copy(entry, rf.Logs[rf.matchIndex[i]+1-rf.Lastindex:])
			nargs := AppendEntriesRPC{
				Term:         rf.Term,
				LeaderId:     rf.me,
				PrevLogIndex: rf.matchIndex[i],
				PrevLogTerm:  rf.getLogTerm(rf.matchIndex[i]),
				LeaderCommit: rf.CommitIndex,
				Entries:      entry,
			}
			nreply := AppendRes{}
			go rf.SyncLog(i, &nargs, &nreply)
			//go rf.SendHeartBeat(i, &nargs, &nreply)
		}
		// entry := make([]Log,1)
		// copy(entry, rf.Logs[len(rf.Logs)-1:])
		// //term := rf.Logs[rf.matchIndex[i]-rf.Lastindex].Term
		// // if rf.matchIndex[i] == rf.Lastindex {
		// // 	term = rf.LastTerm
		// // }
		// nargs := AppendEntriesRPC{
		// 	Term:         rf.Term,
		// 	LeaderId:     rf.me,
		// 	PrevLogIndex: rf.getLastIndex()-1,
		// 	PrevLogTerm:  rf.getLastLogTerm(),
		// 	LeaderCommit: rf.CommitIndex,
		// 	Entries:      entry,
		// }
		// nreply := AppendRes{}
		// go rf.SendHeartBeat(i, &nargs, &nreply)
		i++

	}

	return len(rf.Logs) - 1 + rf.Lastindex, newLog.Term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) CallInstallsnapshop(server int, args *InstallSnapshotRPC, reply *InstallSnapshotReply) {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	if !ok {
	
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.Term {
		rf.Term = reply.Term
		rf.State = Follower
		rf.VotedFor = -1
		rf.persist()
	} else {
		if rf.matchIndex[server] < args.LastIncludeIndex {
			rf.matchIndex[server] = args.LastIncludeIndex
			rf.UpdateCommit()
		}
		if rf.nextIndex[server] < args.LastIncludeIndex+1 {
			rf.nextIndex[server] = args.LastIncludeIndex + 1
		}

	}
}

func (rf *Raft) apply() {
	for !rf.killed() {
		rf.mu.Lock()
		oldApply := rf.LastApplied
		oldCommit := rf.CommitIndex

		//after crash
		if oldApply < rf.Lastindex {
			rf.LastApplied = rf.Lastindex
			rf.CommitIndex = rf.Lastindex
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 30)
			continue
		}
		if oldCommit < rf.Lastindex {

			rf.CommitIndex = rf.Lastindex
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 30)
			continue
		}
		
		if oldApply == oldCommit || (oldCommit-oldApply) >= len(rf.Logs) {
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 5)
			continue
		}
		entry := make([]Log, oldCommit-oldApply)

		copy(entry, rf.Logs[oldApply+1-rf.Lastindex:oldCommit+1-rf.Lastindex])

		rf.mu.Unlock()
		for key, value := range entry {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				CommandIndex: key + oldApply + 1,
				Command:      value.Command,
			}
		
		}

		rf.mu.Lock()
		if rf.LastApplied < oldCommit {
			rf.LastApplied = oldCommit
		}
		if rf.LastApplied > rf.CommitIndex {
			rf.CommitIndex = rf.LastApplied
		}

		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 30)
	}

}

func (rf *Raft) SetLeader() {

	rf.State = Leader
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.matchIndex[i] = 0
	

		rf.nextIndex[i] = rf.getLastIndex() + 1
	}
	

	rf.matchIndex[rf.me] = rf.getLastIndex()
	rf.nextIndex[rf.me] = rf.getLastIndex() + 1
	rf.Broadcast()

}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	rf.State = Candidate
	i := 0
	count := 1
	res := 1
	rf.Term += 1
	rf.VotedFor = rf.me
	rf.persist()
	currentTern := rf.Term
	args := RequestVoteArgs{
		Term:         rf.Term,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	for i < len(rf.peers) {

		reply := RequestVoteReply{}
		if i != rf.me {
			go rf.sendRequestVote(i, &args, &reply, &count, &res)
		}
		i++
	}
	rf.mu.Unlock()
	for {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		if rf.State != Candidate || currentTern != rf.Term {
			rf.mu.Unlock()
			return
		}
		if res <= len(rf.peers)/2 {
			//rf.State=Follower
			if res+len(rf.peers)-count <= len(rf.peers)/2 {
				rf.mu.Unlock()
				return
			}
		} else {
			
			rf.SetLeader()
			//	UpdateCommit()
			rf.mu.Unlock()
			return
		}
		if count == len(rf.peers) {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

	}


}

func (rf *Raft) HeartBeat(args *AppendEntriesRPC, reply *AppendRes) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	if args.Term < rf.Term {
		reply.Term = rf.Term
	
		return
	} else if args.Term > rf.Term {
		reply.Term = args.Term
		rf.Term = args.Term
	
		rf.Time = time.Now()
		rf.State = Follower
		rf.VotedFor = -1
		rf.persist()

	} else {

		//term equal
		rf.Time = time.Now()
		rf.State = Follower
		reply.Term = args.Term
	}

	//lack some logs
	if rf.getLastIndex() < args.PrevLogIndex {
		
		reply.Index = rf.getLastIndex()
		return
	}

	if rf.Lastindex > args.PrevLogIndex {
		if args.PrevLogIndex+len(args.Entries) <= rf.Lastindex {
			reply.Index = rf.Lastindex
			return
		}
		args.PrevLogTerm = args.Entries[rf.Lastindex-args.PrevLogIndex-1].Term
		args.Entries = args.Entries[rf.Lastindex-args.PrevLogIndex:]
		args.PrevLogIndex = rf.Lastindex
	}

	
	if args.PrevLogTerm != rf.getLogTerm(args.PrevLogIndex) {
	
		reply.Index = rf.LastApplied
		if reply.Index > rf.Lastindex {
			reply.Index = rf.Lastindex
		}
		if reply.Index > args.PrevLogIndex-1 {
			reply.Index = args.PrevLogIndex - 1
		}
		return
	}
	reply.Success = true

	//latest condition
	if rf.getLastIndex() == args.PrevLogIndex && args.PrevLogTerm == rf.getLastLogTerm() {
		if args.LeaderCommit > rf.CommitIndex {
			tmp := rf.getLastIndex()
			if tmp > args.LeaderCommit {
				tmp = args.LeaderCommit
			}
			rf.CommitIndex = tmp
		
		}
	}
	//heart beat
	if len(args.Entries) == 0 {
		return
	}

	
	if rf.getLastIndex() >= args.PrevLogIndex+len(args.Entries) && rf.getLogTerm(args.PrevLogIndex+len(args.Entries)) == args.Entries[len(args.Entries)-1].Term {
		return
	}
	i := args.PrevLogIndex + 1

	for i <= rf.getLastIndex() && i-args.PrevLogIndex-1 < len(args.Entries) {
		break
		
	}
	if i-args.PrevLogIndex-1 >= len(args.Entries) {
		return
	}

	rf.Logs = rf.Logs[:i-rf.Lastindex]
	rf.Logs = append(rf.Logs, args.Entries[i-args.PrevLogIndex-1:]...)
	if args.LeaderCommit > rf.CommitIndex {
		tmp := rf.getLastIndex()
		if tmp > args.LeaderCommit {
			tmp = args.LeaderCommit
		}
		rf.CommitIndex = tmp
		
	}
	rf.persist()
	

}

func (rf *Raft) SendHeartBeat(server int, args *AppendEntriesRPC, reply *AppendRes) {
	ok := rf.peers[server].Call("Raft.HeartBeat", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.State != Leader {
		return
	}
	if !reply.Success {
		if reply.Term > rf.Term {
			rf.Term = reply.Term
			rf.State = Follower
			rf.VotedFor = -1
			
			rf.persist()
		} else {
			args.PrevLogIndex = reply.Index
			if args.PrevLogIndex < 0 {
				return
			}
			if args.PrevLogIndex-rf.Lastindex < 0 {
				margs := InstallSnapshotRPC{
					Term:             rf.Term,
					LeaderId:         rf.me,
					LastIncludeIndex: rf.Lastindex,
					LastIncludeTerm:  rf.LastTerm,
					Data:             rf.persister.snapshot,
				}
			
				mreply := InstallSnapshotReply{}
				go rf.CallInstallsnapshop(server, &margs, &mreply)
				return
			} else {
				args.PrevLogTerm = rf.Logs[args.PrevLogIndex-rf.Lastindex].Term
				if args.PrevLogIndex == rf.Lastindex && rf.Lastindex != 0 {
					args.PrevLogTerm = rf.LastTerm
				}
				entry := make([]Log, rf.getLastIndex()-args.PrevLogIndex)
				copy(entry, rf.Logs[args.PrevLogIndex-rf.Lastindex+1:])
				args.Entries = entry
				go rf.SyncLog(server, args, reply)
			}
		}

	} else {
		if rf.matchIndex[server] < args.PrevLogIndex {
			rf.matchIndex[server] = args.PrevLogIndex
			rf.UpdateCommit()
		
		}
		if rf.nextIndex[server] < args.PrevLogIndex+len(args.Entries)+1 {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1

		}
		

	}
}

type ByKey []int

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i] < a[j] }

func (rf *Raft) UpdateCommit() {

	if rf.State != Leader {

		return
	}
	commit := make(ByKey, len(rf.peers))
	for key, value := range rf.matchIndex {
		commit[key] = value
	}
	sort.Sort(ByKey(commit))

	if commit[len(rf.peers)/2] >= rf.Lastindex && rf.Logs[commit[len(rf.peers)/2]-rf.Lastindex].Term == rf.Term && commit[len(rf.peers)/2] > rf.CommitIndex {
		rf.CommitIndex = commit[len(rf.peers)/2]
	}

}

func (rf *Raft) Broadcast() {

	if rf.State == Leader {
		i := 0
		
		for i < len(rf.peers) {
			if i == rf.me {
				i++
				continue
			}
		

			if rf.matchIndex[i] < rf.Lastindex {
				margs := InstallSnapshotRPC{
					Term:             rf.Term,
					LeaderId:         rf.me,
					LastIncludeIndex: rf.Lastindex,
					LastIncludeTerm:  rf.LastTerm,
					Data:             rf.persister.snapshot,
				}
				mreply := InstallSnapshotReply{}
				
				go rf.CallInstallsnapshop(i, &margs, &mreply)

			} else {
				entry := make([]Log, rf.getLastIndex()-rf.matchIndex[i])
				copy(entry, rf.Logs[rf.matchIndex[i]+1-rf.Lastindex:])
			
				nargs := AppendEntriesRPC{
					Term:         rf.Term,
					LeaderId:     rf.me,
					PrevLogIndex: rf.matchIndex[i],
					PrevLogTerm:  rf.getLogTerm(rf.matchIndex[i]),
					LeaderCommit: rf.CommitIndex,
					Entries:      entry,
				}
				nreply := AppendRes{}
				go rf.SyncLog(i, &nargs, &nreply)
			}


			i++
		}
	}
}

func (rf *Raft) ticker() {
	go rf.apply()
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		if time.Since(rf.Time) > time.Duration(100+rand.Intn(300))*time.Millisecond && rf.State != Leader {
			go rf.StartElection()
		}

		if rf.State == Leader {
			i := 0
			prelogindex := rf.getLastIndex()
			prelogterm := rf.getLastLogTerm()
			rf.UpdateCommit()

			for i < len(rf.peers) {
				if i == rf.me {
					i++
					continue
				}
				args := AppendEntriesRPC{
					Term:         rf.Term,
					LeaderId:     rf.me,
					LeaderCommit: rf.CommitIndex,
					PrevLogIndex: prelogindex,
					PrevLogTerm:  prelogterm,
			
				}
				reply := AppendRes{}
				if (rf.nextIndex[i] <= prelogindex || rf.nextIndex[i]-rf.matchIndex[i] != 1) && rf.getLastIndex() != 0 {
					if rf.matchIndex[i] < rf.Lastindex {
						margs := InstallSnapshotRPC{
							Term:             rf.Term,
							LeaderId:         rf.me,
							LastIncludeIndex: rf.Lastindex,
							LastIncludeTerm:  rf.LastTerm,
							Data:             rf.persister.snapshot,
						}
						mreply := InstallSnapshotReply{}
			
						go rf.CallInstallsnapshop(i, &margs, &mreply)

					} else {
						entry := make([]Log, rf.getLastIndex()-rf.matchIndex[i])
						copy(entry, rf.Logs[rf.matchIndex[i]+1-rf.Lastindex:])
	
						nargs := AppendEntriesRPC{
							Term:         rf.Term,
							LeaderId:     rf.me,
							PrevLogIndex: rf.matchIndex[i],
							PrevLogTerm:  rf.getLogTerm(rf.matchIndex[i]),
							LeaderCommit: rf.CommitIndex,
							Entries:      entry,
						}
						nreply := AppendRes{}
						go rf.SyncLog(i, &nargs, &nreply)
					}

				} else {
					go rf.SendHeartBeat(i, &args, &reply)
				}

				i++
			}
			
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		rf.mu.Unlock()

		ms := 30 + (rand.Int63() % 30)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	log := Log{
		Term: 0,
	}
	rf.Time = time.Now()
	rf.State = Follower
	rf.VotedFor = -1
	rf.Logs = make([]Log, 0)

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh

	rf.Logs = append(rf.Logs, log)
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()

	return rf
}
