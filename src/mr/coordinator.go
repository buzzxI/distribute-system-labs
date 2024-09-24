package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/rpc"
	"os"
	"path/filepath"
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

type FileBundle struct {
	files     []TargetFile
	fileToIdx map[string]int
	donePool  map[int]bool
	finished  bool
}

type Coordinator struct {
	// Your definitions here.
	nextWorkerId      int
	lock              sync.Mutex
	nReduce           int
	inputFiles        FileBundle
	intermediateFiles FileBundle
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

func RequestForTargetFile(bundle *FileBundle, workerId int) *TargetFile {
	if bundle.finished {
		return nil
	}

	for i, j := 0, workerId%len(bundle.files); i < len(bundle.files); i, j = i+1, j+1 {
		if j == len(bundle.files) {
			j = 0
		}
		file := &bundle.files[j]
		fmt.Printf("first: worker %v try to lock %v\n", workerId, file.fileName)
		// file state is completed, skip
		if file.state == Completed {
			continue
		}
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

	// check files to complete => block, until all files are completed
	for i, j := 0, workerId%len(bundle.files); i <= len(bundle.files); i, j = i+1, j+1 {
		if j == len(bundle.files) {
			j = 0
		}
		file := &bundle.files[j]
		// complete work cannot be in progress again
		if file.state == Completed {
			continue
		}
		fmt.Printf("worker %v try to lock %v\n", workerId, file.fileName)
		file.lock.Lock()
		fmt.Printf("worker %v locked %v\n", workerId, file.fileName)
		// task has not started or timeout
		if file.state == InProgress {
			i = 0
			lastTime := file.lastExecuteTime
			sinceLast := time.Since(lastTime)
			if sinceLast >= MAX_EXECUTE_TIME {
				// redo task immediately
				fmt.Printf("worker %v redo task %v\n", workerId, file.fileName)
				file.lastExecuteTime = time.Now()
				file.condition.Broadcast() // wake up the waiting worker
				file.lock.Unlock()
				fmt.Printf("worker %v release the lock %v\n", workerId, file.fileName)
				return file
			} else {
				// wait to redo task (wake up by other worker or monitor)
				fmt.Printf("worker %v wait to redo task %v\n", workerId, file.fileName)
				file.condition.Wait()
				// current task has not been taken by other worker
				if file.state == InProgress && file.lastExecuteTime == lastTime {
					file.lastExecuteTime = time.Now()
					file.lock.Unlock()
					fmt.Printf("worker %v release the lock %v\n", workerId, file.fileName)
					return file
				}
			}
		}
		file.lock.Unlock()
		fmt.Printf("worker %v unlock %v\n", workerId, file.fileName)
	}

	return nil
}

func (c *Coordinator) TryToFinishFileState(request *RPCRequset) {
	var bundle *FileBundle
	if request.TaskType == Map {
		bundle = &c.inputFiles
	} else {
		bundle = &c.intermediateFiles
	}

	if idx, exist := bundle.fileToIdx[request.FinishedFile]; exist {
		file := &bundle.files[idx]
		fmt.Printf("worker %v try to finish %v\n", request.WorkerId, request.FinishedFile)
		file.lock.Lock()
		// task will be marked as finished, only if the task has not timeout
		if file.state != Completed && time.Since(file.lastExecuteTime) < MAX_EXECUTE_TIME {
			fmt.Printf("worker %v finish %v\n", request.WorkerId, request.FinishedFile)
			file.state = Completed
			// mark the task as finished
			bundle.donePool[idx] = true

			fmt.Printf("%v %v\n", request.FinishedFile, bundle.donePool)

			if len(bundle.donePool) == len(bundle.files) {
				bundle.finished = true
			}

			if request.TaskType == Reduce {
				newPath := fmt.Sprintf("mr-out-%d", idx)
				err := os.Rename(request.IntermediateFile, newPath)
				if err != nil {
					log.Fatalf("cannot rename %v", err)
				}
			}
		} else {
			fmt.Printf("worker %v timeout %v\n", request.WorkerId, request.FinishedFile)
			if request.TaskType == Map {
				// map task generate a bunch of intermediate file
				wildcard := fmt.Sprintf("%s*", request.IntermediateFile)
				files, err := filepath.Glob(wildcard)
				if err != nil {
					log.Fatalf("cannot find %v", err)
				}
				for _, f := range files {
					err = os.Remove(f)
					if err != nil {
						log.Fatalf("cannot remove %v", err)
					}
				}
			} else {
				// reduce task generate one intermediate file
				err := os.Remove(request.IntermediateFile)
				if err != nil {
					log.Fatalf("cannot remove %v", err)
				}
			}
		}
		// wake waiting worker
		file.condition.Broadcast()
		file.lock.Unlock()
		fmt.Printf("worker %v return %v\n", request.WorkerId, request.FinishedFile)
	}
}

func (c *Coordinator) RequestForTask(request *RPCRequset, response *RPCResponse) error {
	fmt.Printf("worker %v request task, task type %v\n", request.WorkerId, request.TaskType)

	c.TryToFinishFileState(request)

	mapTaskFile := RequestForTargetFile(&c.inputFiles, request.WorkerId)
	if mapTaskFile != nil {
		response.TaskType = Map
		response.InputFile = mapTaskFile.fileName
		fmt.Printf("worker %v assign map task %v\n", request.WorkerId, mapTaskFile.fileName)
		return nil
	}

	reduceTask := RequestForTargetFile(&c.intermediateFiles, request.WorkerId)

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
	return c.intermediateFiles.finished
}

func (c *Coordinator) monitor() {
	for {
		time.Sleep(time.Second)

		if c.Done() {
			break
		}

		for i := 0; i < len(c.inputFiles.files) && !c.inputFiles.finished; i++ {
			// if c.inputFiles.files[i].state == Completed {
			// 	continue
			// }
			file := &c.inputFiles.files[i]

			file.lock.Lock()
			if file.state == InProgress && time.Since(file.lastExecuteTime) > MAX_EXECUTE_TIME {
				file.condition.Broadcast()
			}
			file.lock.Unlock()
		}

		for i := 0; i < len(c.intermediateFiles.files) && !c.intermediateFiles.finished; i++ {
			// if c.inputFiles.files[i].state == Completed {
			// 	continue
			// }
			file := &c.intermediateFiles.files[i]

			file.lock.Lock()
			if file.state == InProgress && time.Since(file.lastExecuteTime) > MAX_EXECUTE_TIME {
				file.condition.Broadcast()
			}
			file.lock.Unlock()
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{}
	c.nextWorkerId = 0
	c.nReduce = nReduce
	c.inputFiles = FileBundle{files: make([]TargetFile, len(files)), fileToIdx: make(map[string]int, len(files)), donePool: make(map[int]bool)}
	c.intermediateFiles = FileBundle{files: make([]TargetFile, nReduce), fileToIdx: make(map[string]int, nReduce), donePool: make(map[int]bool)}

	for idx, file := range files {
		c.inputFiles.files[idx] = TargetFile{}
		c.inputFiles.files[idx].fileName = file
		c.inputFiles.files[idx].state = NotStarted
		c.inputFiles.files[idx].lastExecuteTime = time.Now()
		c.inputFiles.files[idx].lock = sync.Mutex{}
		c.inputFiles.files[idx].condition = sync.NewCond(&c.inputFiles.files[idx].lock)
		c.inputFiles.fileToIdx[file] = idx
	}

	fmt.Printf("input files %v\n", c.inputFiles.fileToIdx)

	for i := 0; i < nReduce; i++ {
		fileName := fmt.Sprintf("%d", i)
		c.intermediateFiles.files[i] = TargetFile{}
		c.intermediateFiles.files[i].fileName = fileName
		c.intermediateFiles.files[i].state = NotStarted
		c.intermediateFiles.files[i].lastExecuteTime = time.Now()
		c.intermediateFiles.files[i].lock = sync.Mutex{}
		c.intermediateFiles.files[i].condition = sync.NewCond(&c.intermediateFiles.files[i].lock)
		c.intermediateFiles.fileToIdx[fileName] = i
	}

	go func() {
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(1)
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	go c.monitor()

	c.server()
	return &c
}
