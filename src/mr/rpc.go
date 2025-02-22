package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type GetMapArgs struct{}
type GetMapReply struct {
	Filename string
	NReduce  int
}

type GetReduceArgs struct{}
type GetReduceReply struct {
	wait      bool
	Filenames []string
	ReduceN   int
}

type MarkMapTaskDoneArgs struct {
	Filename string
}
type MarkMapTaskDoneReply struct{}

type MarkReduceTaskDoneArgs struct {
	ReduceN int
}
type MarkReduceTaskDoneReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
