package mr

import (
	"os"
	"time"
)
import "strconv"

// use for args and reply

type Task struct {
	ExecutionFiles []string
	TaskType       TaskType
	NReduce        int
	TaskId         int
	StartTime      time.Time
}

type TaskArgs struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
