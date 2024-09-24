package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/rpc"
	"os"
	"runtime"
	"sync"
	"time"
)

const MAX_EXECUTE_TIME = 10 * time.Second

type TaskState int

const (
	NotStarted TaskState = iota
	InProgress
	Completed
)

type TargetFile struct {
	fileName        string
	state           TaskState
	lastExecuteTime time.Time
	lock            sync.Mutex // a lock for condition
	condition       *sync.Cond // worker may wait for
}

type Coordinator struct {
	// Your definitions here.
	nextWorkerId              int
	lock                      sync.Mutex
	nReduce                   int
	inputFiles                []TargetFile
	intputFileNameToIdx       map[string]int // map input file name to index
	intermediateFiles         []TargetFile
	intermediateFileNameToIdx map[string]int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) InitialRequest(request *RPCRequset, response *RPCResponse) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	response.WorkerId = c.nextWorkerId
	c.nextWorkerId++
	response.NReduce = c.nReduce
	return nil
}

// wait for a condition with timeout
// return true if the condition is met, otherwise return false
// this function will always give-up lock before return
func TimeoutWait(cond *sync.Cond, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	// a channel to notify the condition is met
	done := make(chan bool)
	go func() {
		cond.Wait()
		cond.L.Unlock() // unlock the lock
		done <- true
	}()

	// wait for the condition or timeout
	select {
	case <-timer.C:
		{
			cond.Broadcast() // wake up nested goroutine
			return false
		}
	case <-done:
		return true
	}
}

// return a task to finish
func RequestForTargetFile(inputFiles []TargetFile, workerId int) *TargetFile {
	for i, j := 0, workerId%len(inputFiles); i < len(inputFiles); i, j = i+1, j+1 {
		if j == len(inputFiles) {
			j = 0
		}
		file := &inputFiles[j]
		fmt.Printf("first: worker %v try to lock %v\n", workerId, file.fileName)
		// file state is completed, skip
		// if file.state == Completed {
		// 	continue
		// }
		file.lock.Lock()
		fmt.Printf("first: worker %v locked %v\n", workerId, file.fileName)
		if file.state == NotStarted || (file.state == InProgress && time.Since(file.lastExecuteTime) > MAX_EXECUTE_TIME) {
			file.state = InProgress
			file.lastExecuteTime = time.Now()
			file.lock.Unlock()
			fmt.Printf("first: worker %v get task %v\n", workerId, file.fileName)
			return file
		}
		file.lock.Unlock()
		fmt.Printf("first: worker %v unlock %v\n", workerId, file.fileName)
	}

	for i, j := 0, workerId%len(inputFiles); i < len(inputFiles); i, j = i+1, j+1 {
		if j == len(inputFiles) {
			j = 0
		}
		inputFile := &inputFiles[j]
		// complete work cannot be in progress again
		// if inputFile.state == Completed {
		// 	continue
		// }
		fmt.Printf("worker %v try to lock %v\n", workerId, inputFile.fileName)
		inputFile.lock.Lock()
		fmt.Printf("worker %v locked %v\n", workerId, inputFile.fileName)
		// task has not started or timeout
		if inputFile.state == InProgress {
			i = 0
			lastTime := inputFile.lastExecuteTime
			sinceLast := time.Since(lastTime)
			if sinceLast >= MAX_EXECUTE_TIME {
				// redo task immediately
				fmt.Printf("worker %v redo task %v\n", workerId, inputFile.fileName)
				inputFile.lastExecuteTime = time.Now()
				inputFile.condition.Broadcast() // wake up the waiting worker
				inputFile.lock.Unlock()
				fmt.Printf("worker %v release the lock %v\n", workerId, inputFile.fileName)
				return inputFile
			} else {
				// wait for the task to finish
				fmt.Printf("worker %v wait to redo task %v\n", workerId, inputFile.fileName)
				finished := TimeoutWait(inputFile.condition, MAX_EXECUTE_TIME-sinceLast)
				if !finished {
					// task timeout (lost lock)
					fmt.Printf("worker %v retry to lock %v\n", workerId, inputFile.fileName)
					inputFile.lock.Lock()
					fmt.Printf("worker %v relocked %v\n", workerId, inputFile.fileName)
					// current task has not been taken by other worker
					if inputFile.lastExecuteTime == lastTime {
						inputFile.lastExecuteTime = time.Now()
						inputFile.condition.Broadcast() // wake up the waiting worker
						inputFile.lock.Unlock()
						fmt.Printf("worker %v release the lock %v\n", workerId, inputFile.fileName)
						return inputFile
					}
				}
				continue
			}
		}
		inputFile.lock.Unlock()
		fmt.Printf("worker %v unlock %v\n", workerId, inputFile.fileName)
	}

	return nil
}

func TryToFinishFileState(filename string, files []TargetFile, fileToIdx map[string]int, workerId int) {
	if _, exist := fileToIdx[filename]; exist {
		idx := fileToIdx[filename]
		file := &files[idx]
		fmt.Printf("worker %v try to finish %v\n", workerId, filename)
		file.lock.Lock()
		fmt.Printf("worker %v finish %v\n", workerId, filename)
		// task will be marked as finished, only if the task has not timeout
		if file.state != Completed && time.Since(file.lastExecuteTime) < MAX_EXECUTE_TIME {
			file.state = Completed
			// wake waiting worker
			file.condition.Broadcast()
		}
		file.lock.Unlock()
		fmt.Printf("worker %v return %v\n", workerId, filename)
	}
}

func (c *Coordinator) RequestForTask(request *RPCRequset, response *RPCResponse) error {
	fmt.Printf("worker %v request task, task type %v\n", request.WorkerId, request.TaskType)

	if request.TaskType == Map {
		TryToFinishFileState(request.FinishedFile, c.inputFiles, c.intputFileNameToIdx, request.WorkerId)
	} else if request.TaskType == Reduce {
		TryToFinishFileState(request.FinishedFile, c.intermediateFiles, c.intermediateFileNameToIdx, request.WorkerId)
	}

	mapTaskFile := RequestForTargetFile(c.inputFiles, request.WorkerId)
	if mapTaskFile != nil {
		response.TaskType = Map
		response.InputFile = mapTaskFile.fileName
		fmt.Printf("worker %v assign map task %v\n", request.WorkerId, mapTaskFile.fileName)
		return nil
	}

	reduceTask := RequestForTargetFile(c.intermediateFiles, request.WorkerId)

	if reduceTask != nil {
		response.TaskType = Reduce
		response.InputFile = reduceTask.fileName
		fmt.Printf("worker %v assign reduce task %v\n", request.WorkerId, reduceTask.fileName)
		return nil
	}

	response.TaskType = None
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

// main/mrcoordinator.go calls Done() periodically to find out => unsync state
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	for i := 0; i < len(c.intermediateFiles); i++ {
		if c.intermediateFiles[i].state != Completed {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{}
	c.nextWorkerId = 0
	c.nReduce = nReduce
	c.inputFiles = make([]TargetFile, len(files))
	c.intputFileNameToIdx = make(map[string]int, len(files))
	c.intermediateFiles = make([]TargetFile, nReduce)
	c.intermediateFileNameToIdx = make(map[string]int, nReduce)

	for idx, file := range files {
		c.inputFiles[idx] = TargetFile{}
		c.inputFiles[idx].fileName = file
		c.inputFiles[idx].state = NotStarted
		c.inputFiles[idx].lastExecuteTime = time.Now()
		c.inputFiles[idx].lock = sync.Mutex{}
		c.inputFiles[idx].condition = sync.NewCond(&c.inputFiles[idx].lock)
		c.intputFileNameToIdx[file] = idx
	}

	for i := 0; i < nReduce; i++ {
		fileName := fmt.Sprintf("%d", i)
		c.intermediateFiles[i] = TargetFile{}
		c.intermediateFiles[i].fileName = fileName
		c.intermediateFiles[i].state = NotStarted
		c.intermediateFiles[i].lastExecuteTime = time.Now()
		c.intermediateFiles[i].lock = sync.Mutex{}
		c.intermediateFiles[i].condition = sync.NewCond(&c.intermediateFiles[i].lock)
		c.intermediateFileNameToIdx[fileName] = i
	}

	go func() {
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(1)
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	c.server()
	return &c
}
