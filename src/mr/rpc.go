package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "sync"
import "time"
//
// example to show how to declare the arguments
// and reply for an RPC.
//


type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Task struct {
	TaskType int
	Filename string
	NReduce int
	TaskId int
	Finished bool
	Start time.Time
}


const Waiting = 3
const Map = 1 
const Reduce = 2

var mutex sync.Mutex

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
