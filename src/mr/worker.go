package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	request := RPCRequset{}
	request.TaskType = None

	// initial request: get worker id
	response := InitialRequest(request)
	nReduce := response.NReduce
	// set worker id
	request.WorkerId = response.WorkerId

	for {
		// request for task
		response := RequestForTask(request)
		// no task
		if response.TaskType == None {
			break
		}

		request.TaskType = response.TaskType
		if response.TaskType == 1 {
			MapTask(mapf, response.InputFile, nReduce, request.WorkerId)
		} else if response.TaskType == 2 {
			intermediate, err := strconv.Atoi(response.InputFile)
			if err != nil {
				log.Fatalf("cannot convert %v", response.InputFile)
			}
			ReduceTask(reducef, intermediate)
		}
		request.FinishedFile = response.InputFile
	}
}

func MapTask(mapf func(string, string) []KeyValue, filename string, nReduce int, workerId int) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	intermediate := mapf(filename, string(content))

	// use temp file to store intermediate result -> rename to final file after write done
	tempPathName := fmt.Sprintf("mr/worker-%d", workerId)
	// define the temp directory
	subDir := filepath.Join(os.TempDir(), tempPathName)

	// Create the subdirectory if it doesn't exist
	err = os.MkdirAll(subDir, 0755) // Permissions: read/write/execute for owner, read/execute for others
	if err != nil {
		log.Fatal("Error creating subdirectory:", err)
	}

	tempFiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)

	for i := 0; i < nReduce; i++ {
		tempFile, err := os.CreateTemp(subDir, "mr-intermediate-*")
		if err != nil {
			log.Fatalf("cannot open %v", err)
		}

		tempFiles[i] = tempFile
		encoders[i] = json.NewEncoder(tempFile)
	}

	for _, kv := range intermediate {
		idx := ihash(kv.Key) % nReduce
		err := encoders[idx].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v", err)
		}
	}

	hashFile := ihash(filename)

	for i := 0; i < nReduce; i++ {
		// rename to final file -> the struct of intermediate file is "mr-intermediate-{map worker id}-{filename}-{reduce id}"
		err = os.Rename(tempFiles[i].Name(), fmt.Sprintf("mr-intermediate-%d-%d-%d", workerId, hashFile, i))
		if err != nil {
			log.Fatalf("cannot rename %v", err)
		}
		tempFiles[i].Close()
	}
}

func ReduceTask(reducef func(string, []string) string, intermediate int) {
	ifilename := fmt.Sprintf("mr-intermediate-*-%d", intermediate)
	ofilename := fmt.Sprintf("mr-out-%d", intermediate)

	files, err := filepath.Glob(ifilename)
	if err != nil {
		log.Fatalf("cannot open %v", ifilename)
	}

	kvs := []KeyValue{}
	// read all intermediate files
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kvs))

	ofile, err := os.Create(ofilename)
	if err != nil {
		log.Fatalf("cannot create %v", err)
	}
	defer ofile.Close()

	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)

		i = j
	}

}

func InitialRequest(request RPCRequset) RPCResponse {
	response := RPCResponse{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.RequestForTask" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.InitialRequest", &request, &response)
	if ok {
		fmt.Printf("initial worker id %v\n", response.WorkerId)
	} else {
		fmt.Printf("call failed!\n")
	}
	return response
}

func RequestForTask(request RPCRequset) RPCResponse {

	// declare a reply structure.
	response := RPCResponse{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.RequestForTask" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.RequestForTask", &request, &response)
	if ok {
		fmt.Printf("worker %v task type %v\n", request.WorkerId, response.TaskType)
	} else {
		fmt.Printf("worker %v call failed!\n", request.WorkerId)
		// the worker assume the coordinator is down
		response.TaskType = None
	}
	return response
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
