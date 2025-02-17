package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Task struct {
	TaskType  TaskType
	TaskId    int
	ReduceNum int
	FileNames []string
}

type TaskArgs struct {
	TaskId int
}

type TaskType int

type Phase int

type State int

const (
	MapTask TaskType = iota
	ReduceTask
	WaittingTask
	ExitTask
)

const (
	MapPhase Phase = iota
	ReducePhase
	AllDone
)

const (
	Working State = iota
	Waitting
	Done
)

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
