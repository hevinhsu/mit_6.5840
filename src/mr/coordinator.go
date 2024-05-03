package mr

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

type JobPhase int

const (
	InitPhase           JobPhase = iota // init status.
	DoMapTasksPhase                     // when split execution files into task job completed, status will come to Map Phase
	DoReduceTasksPhase                  // when all Map task are completed, status will come to Reduce Phase
	NotifyCompletePhase                 //when all reduce task completed, status will come to NotifyComplete and handle rpc with AllComplete
	CompletePhase                       // when handled all AllComplete rpc call, coordinator will exit(0)
)

type Coordinator struct {
	nReduce       int // for hashing in worker
	inputFileSize int // map tasks size
	sync.Mutex
	jobPhase          JobPhase      // for state machine
	taskChannel       chan *Task    // store tasks for worker
	executionTable    map[int]*Task // for timeout detect
	allTaskDone       bool          // to recognize all task is done
	taskCounter       int           // to prevent
	intermediateFiles []string
}

func (c *Coordinator) isTaskComplete() bool {
	c.Lock()
	defer func() {
		c.Unlock()
	}()
	if 0 == c.taskCounter {
		return true
	} else {
		//fmt.Printf("[isTaskComplete] wait for %d task complete.\n", c.taskCounter)
		return false
	}
}

// rpc methods

func (c *Coordinator) PullTask(args *TaskArgs, reply *Task) error {
	c.Lock()
	defer func() {
		c.Unlock()
	}()
	if 0 == len(c.taskChannel) {
		reply.TaskType = SleepType
		return nil
	}

	task := <-c.taskChannel
	task.StartTime = time.Now()
	reply.ExecutionFiles = task.ExecutionFiles
	reply.NReduce = task.NReduce
	reply.TaskType = task.TaskType
	reply.TaskId = task.TaskId
	reply.StartTime = task.StartTime

	c.executionTable[task.TaskId] = task
	//fmt.Printf("[PullTask] [%v] task: %v is asigned to worker\n", task.TaskType, task.TaskId)

	return nil
}

func (c *Coordinator) NotifyTaskComplete(task *Task, args *Task) error {
	c.Lock()
	defer func() {
		c.Unlock()
	}()

	exec, ok := c.executionTable[task.TaskId]
	if ok {
		if task.StartTime.Equal(exec.StartTime) {
			delete(c.executionTable, task.TaskId)
			c.taskCounter--
			if task.TaskType == DoMapType {
				c.intermediateFiles = append(c.intermediateFiles, task.ExecutionFiles...)
			}
			//fmt.Printf("[NotifyTaskComplete] [%v] task: %v is complete. wait for: %v tasks\n", task.TaskType, task.TaskId, c.taskCounter)

		} else {
			//fmt.Printf("[NotifyTaskComplete] received [%v] task: %v, but it's timeout and be aborted.  diff: %v\n", task.TaskType, task.TaskId, time.Since(task.StartTime))
			//fmt.Printf("received msg StartTime: %v, executionTable StartTime: %v\n", task.StartTime, exec.StartTime)
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.

func (c *Coordinator) Done() bool {
	return c.allTaskDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.

func MakeCoordinator(files []string, nReduce int) *Coordinator {

	inputFileSize := len(files)
	channelSize := inputFileSize * nReduce

	for index, file := range files {
		//files[index] = filepath.Base(file)
		files[index] = file // test in wsl
	}

	c := Coordinator{
		nReduce,
		inputFileSize,
		sync.Mutex{},
		InitPhase,
		make(chan *Task, channelSize),
		make(map[int]*Task),
		false,
		0,
		make([]string, inputFileSize*nReduce),
	}

	go jobPhaseMaintainer(&c, files)

	c.server()
	return &c
}

const Timeout = 15 * time.Second

func jobPhaseMaintainer(c *Coordinator, inputFiles []string) {
	canExecute := true
	for canExecute {
		switch c.jobPhase {
		case InitPhase:
			c.transferToDoMapTasksPhase(inputFiles)
			//fmt.Println("[init] init phase complete. transfer to DoMapTasksPhase")
		case DoMapTasksPhase:
			if c.isTaskComplete() {
				c.transferToReduceTasksPhase()
				//fmt.Println("[DoMapTasksPhase] phase complete. transfer to DoReduceTasksPhase")
				break
			}
			c.handleTimeout()
		case DoReduceTasksPhase:
			if c.isTaskComplete() {
				c.transferToNotifyCompletePhase()
				//fmt.Println("[DoReduceTasksPhase] phase complete. transfer to NotifyCompletePhase")
				break
			}
			c.handleTimeout()
		case NotifyCompletePhase:
			if c.isTaskComplete() {
				c.jobPhase = CompletePhase
				//fmt.Println("[NotifyCompletePhase] init phase complete. transfer to CompletePhase")
				break
			}
			c.handleTimeout()
		case CompletePhase:
			c.allTaskDone = true
			canExecute = false
			//fmt.Println("[CompletePhase] coordinator job complete.")
		}

		time.Sleep(time.Second)
	}
}

func (c *Coordinator) transferToNotifyCompletePhase() {
	c.Lock()
	defer func() {
		c.Unlock()
	}()
	for i := 0; i < c.nReduce; i++ {
		task := Task{
			nil,
			AllCompleteType,
			c.nReduce,
			i,
			time.Time{}, // nil
		}
		c.taskChannel <- &task
		c.taskCounter++
	}
	c.jobPhase = NotifyCompletePhase
}

func (c *Coordinator) transferToReduceTasksPhase() {
	c.Lock()
	defer func() {
		c.Unlock()
	}()

	for reduceTaskId := 0; reduceTaskId < c.nReduce; reduceTaskId++ {
		var filenames []string

		pattern := fmt.Sprintf("mr-.*-%d", reduceTaskId)
		regex := regexp.MustCompile(pattern)
		for _, file := range c.intermediateFiles {
			if regex.MatchString(file) {
				filenames = append(filenames, file)
			}
		}

		task := Task{
			filenames,
			DoReduceType,
			c.nReduce,
			reduceTaskId,
			time.Time{}, // nil
		}
		c.taskChannel <- &task
		c.taskCounter++
	}
	c.jobPhase = DoReduceTasksPhase
}

func (c *Coordinator) handleTimeout() {
	c.Lock()
	defer func() {
		c.Unlock()
	}()
	for _, task := range c.executionTable {
		if time.Since(task.StartTime) > Timeout {
			//fmt.Printf("[handleTimeout] find timeout task: %d in phase: %v. exec filename: %v\n", task.TaskId, task.TaskType, task.ExecutionFiles)
			task.StartTime = time.Now()
			c.taskChannel <- task
		}
	}
}

func (c *Coordinator) transferToDoMapTasksPhase(inputFiles []string) {
	c.Lock()
	defer func() {
		c.Unlock()
	}()
	tasks := createTasks(inputFiles, c.nReduce)
	for _, task := range tasks {
		c.taskChannel <- task
		c.taskCounter++
	}
	c.jobPhase = DoMapTasksPhase
}

func createTasks(files []string, nReduce int) []*Task {
	var tasks []*Task
	for index, filename := range files {
		var slice []string
		slice = append(slice, filename)
		task := Task{
			slice,
			DoMapType,
			nReduce,
			index,
			time.Time{}, // nil
		}
		tasks = append(tasks, &task)
	}
	return tasks
}
