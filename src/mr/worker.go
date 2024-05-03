package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// default

type KeyValue struct {
	Key   string
	Value string
}

// default
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// copy from mrsequential.go

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// work path

// main/mrworker.go calls this function.

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	//pid := os.Getpid()
	//fmt.Printf("[%d] worker start.\n", pid)
	//defer func() {
	//	fmt.Printf("[%d] worker closed.\n", pid)
	//}()

	isComplete := false
	for !isComplete {

		task := pullTask()
		//if task.TaskType != SleepType {
		//	fmt.Printf("[worker] [received] [%v] taskId: %v\n", task.TaskType, task.TaskId)
		//}

		switch task.TaskType {
		case AllCompleteType:
			handleComplete(task)
			isComplete = true
		case SleepType:
			handleSleep(SleepSlice)
		case DoMapType:
			HandleMap(task, mapf)
		case DoReduceType:
			HandleReduce(task, reducef)
		}

	}

}

//const tempFileDirFromWorkspace = "/test" // local test
const tempFileDirFromWorkspace = "" // wsl test

//func createTempFolder(path string) {
//	if _, err := os.Stat(path); os.IsNotExist(err) {
//		err := os.MkdirAll(path, fs.ModeDir)
//		if err != nil {
//			log.Fatalf("mkdir failed: %v", err)
//		}
//	}
//}

func HandleMap(task *Task, mapf func(string, string) []KeyValue) {
	workspaceDir, _ := os.Getwd()
	tempFileDir := workspaceDir + tempFileDirFromWorkspace
	//createTempFolder(tempFileDir) // local test

	//fmt.Printf("[worker] handle map for TaskId: %v\n", task.TaskId)

	filenameWithPath := task.ExecutionFiles[0] // wsl test v2
	//filenameWithPath := workspaceDir + "/main/" + task.ExecutionFiles[0] // local test
	var intermediateFilesData = make(map[int][]KeyValue)

	// read content from file
	file, err := os.Open(filenameWithPath)
	if err != nil {
		log.Fatalf("[worker] [map] can not open %v", filenameWithPath)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("[worker] [map] can not read %v", filenameWithPath)
	}
	err = file.Close()
	if err != nil {
		log.Fatalf("[worker] [map] can not close file: %v", filenameWithPath)
	}

	// call map
	kva := mapf(filenameWithPath, string(content))

	// partitioning fallowed by lab_1 hint
	for _, kv := range kva {
		index := ihash(kv.Key) % task.NReduce
		arr := intermediateFilesData[index]
		arr = append(arr, kv)
		intermediateFilesData[index] = arr
	}

	// write result to intermediate files
	var intermediateFiles []string
	for index, fileData := range intermediateFilesData {

		outputName := fmt.Sprintf("mr-%d-%d", task.TaskId, index)
		tempFile, err := os.CreateTemp(tempFileDir, outputName)
		if err != nil {
			log.Fatalf("[worker] [map] can not create tmp file %v. err: %v", outputName, err)
		}
		encoder := json.NewEncoder(tempFile)

		for _, line := range fileData {
			err := encoder.Encode(line)
			if err != nil {
				log.Fatalf("[worker] [map] encode to json error: %v", filenameWithPath)
			}
		}

		// can not use defer keyword to auto close
		// because the method is in for loop.
		// defer will called end of the function call.
		err = tempFile.Close()
		if err != nil {
			log.Fatalf("[worker] [map] can not close tmp file %v", outputName)
		}

		targetTempFileWithPath := tempFileDir + "/" + outputName
		err = os.Rename(tempFile.Name(), targetTempFileWithPath)
		//fmt.Printf("rename %v to %v\n", tempFile.Name(), targetTempFileWithPath)
		if err != nil {
			log.Fatalf("[worker] [map] rename tmp file from: %v to : %v error: %v", tempFile.Name(), targetTempFileWithPath, err)
		}
		intermediateFiles = append(intermediateFiles, outputName)

	}
	task.ExecutionFiles = intermediateFiles
	notifyComplete(task)
}

func HandleReduce(task *Task, reducef func(string, []string) string) {
	//fmt.Printf("[worker] handle reduce for TaskId: %v, exec files: %v\n", task.TaskId, task.ExecutionFiles)

	if 0 == len(task.ExecutionFiles) {
		//fmt.Printf("[worker] handle reduce for TaskId: %v with empty exec files. early return.\n", task.TaskId)
		notifyComplete(task)
		return
	}

	//workspaceDir := "" // wsl test v2
	workspaceDir, _ := os.Getwd()
	tempFileDir := workspaceDir + tempFileDirFromWorkspace

	var intermediate []KeyValue

	// read file data into memory
	for _, filename := range task.ExecutionFiles {

		filename = tempFileDir + "/" + filename

		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("[worker] [reduce] can not open file: %v.\n", filename)
		}

		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		err = file.Close()
		if err != nil {
			log.Fatalf("[worker] [reduce] can not close file: %v", filename)
		}
	}

	// sort data by hint
	sort.Sort(ByKey(intermediate))

	outputFileName := fmt.Sprintf("mr-out-%d", task.TaskId)
	tempFile, err := os.CreateTemp(tempFileDir, outputFileName)
	if err != nil {
		log.Fatalf("[worker] [reduce] can not create tmp output file from : %v. err: %v", outputFileName, err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		_, err := fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			log.Fatalf("[worker] [reduce] can not write to tmp output file from : %v", tempFile)
		}

		i = j
	}

	err = tempFile.Close()
	if err != nil {
		log.Fatalf("[worker] [reduce] can not close tmp file %v", outputFileName)
	}
	targetOutputFileWithPath := outputFileName // wsl testing v2
	//targetOutputFileWithPath := workspaceDir + "/main/" + outputFileName
	err = os.Rename(tempFile.Name(), targetOutputFileWithPath)
	if err != nil {
		log.Fatalf("[worker] [reduce] rename tmp file from: %v to : %v error: %v", tempFile.Name(), targetOutputFileWithPath, err)
	}

	//fmt.Printf("tmp: %v, dest: %v\n", tempFile.Name(), targetOutputFileWithPath)
	//fmt.Printf("source: %v, dest: %v\n", task.ExecutionFiles, targetOutputFileWithPath)

	notifyComplete(task)
}

func handleComplete(task *Task) {
	notifyComplete(task)
}

func handleSleep(timeSlice time.Duration) {
	//fmt.Println("[worker] handle sleep")
	time.Sleep(time.Second * timeSlice)
}

type TaskType string

const (
	SleepType       TaskType      = "sleep"
	DoMapType       TaskType      = "doMap"
	DoReduceType    TaskType      = "doReduce"
	AllCompleteType TaskType      = "complete"
	NilId                         = 255
	SleepSlice      time.Duration = 1
)

func pullTask() *Task {
	task := Task{}
	ok := call("Coordinator.PullTask", &TaskArgs{}, &task)
	if !ok {
		log.Println("[worker] rpc call [PullTask] failed.")
		return &Task{
			nil, SleepType, 0, NilId, time.Time{},
		}
	}
	return &task
}

func notifyComplete(task *Task) bool {
	//defer func() {
	//	fmt.Printf("[worker] [%v] mission %v complete\n", task.TaskType, task.TaskId)
	//}()
	return call("Coordinator.NotifyTaskComplete", task, &TaskArgs{})
}

// default
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("[worker] dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	return false
}

// local testing in wsl

const N = 10

func LocalWorker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	i := 0

	wg := &sync.WaitGroup{}
	wg.Add(N)

	for i < N {
		go func() {
			Worker(mapf, reducef)
			wg.Done()
		}()
		i++
	}
	wg.Wait()
}
