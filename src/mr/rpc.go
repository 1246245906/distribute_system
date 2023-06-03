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

// example to show how to declare the arguments
// and reply for an RPC.
type ReqType int

const (
	REQ_TASK ReqType = iota
	REPORT_RESULT
)

type RepType int

const (
	MAP RepType = iota
	REDUCE
	WAIT
	DONE
	REQUEST_ERROR
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type RequestArgs struct {
	req_type ReqType
	task_seq int // REPORT_RESULT 时使用
	msg      string
}

type Reply struct {
	rep_type  RepType
	task_seq  int
	file_path string
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
