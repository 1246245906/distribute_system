package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func CallForTask() (RepType, int, int, string) {
	args := RequestArgs{REQ_TASK, 0, ""}
	reply := Reply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	// todo: error process
	if ok {
		return reply.ReportType, reply.TaskSeq, reply.NReduce, reply.FilePath
	} else {
		fmt.Printf("call failed!\n")
	}
	return REQUEST_ERROR, 0, 0, ""
}

func DoMap(file_path string, mapf func(string, string) []KeyValue) []KeyValue {
	file_name := filepath.Base(file_path)
	file, err := os.Open(file_name)
	if err != nil {
		log.Fatalf("cannot open %v", file_name)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", file_name)
	}
	fmt.Printf("[Worker][MAP]: %s is beginning!\n", file_name)
	kva := mapf(file_name, string(content))
	fmt.Printf("[Worker][MAP]: %s map succ!\n", file_name)
	return kva
}

func DoHashSplit(kvs []KeyValue, X int, nReduce int, file_dir string) {
	// var out_X [][]KeyValue
	out_X := make([][]KeyValue, nReduce)
	// var Y int
	for _, kv := range kvs {
		Y := ihash(kv.Key) % nReduce
		out_X[Y] = append(out_X[Y], kv)
	}
	// for _, hashed_kvs := range out_X {
	// 	intermedia_output_file_name := fmt.Sprintf("mr-out-%d-%d", X, Y)
	// 	WriteKV2File(hashed_kvs, filepath.Join(file_dir, intermedia_output_file_name))
	// }

	for Y := 0; Y < nReduce; Y += 1 {
		if len(out_X[Y]) > 0 {
			intermedia_output_file_name := fmt.Sprintf("mr-out-%d-%d", X, Y)
			WriteKV2File(out_X[Y], filepath.Join(file_dir, intermedia_output_file_name))
		}
	}
}

func ReportMapResult(task_seq int, out_dir string) {
	args := RequestArgs{REPORT_RESULT, task_seq, out_dir}
	reply := Reply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	// todo: error process
	if ok {
		// reply.Y should be 100.
		fmt.Printf("report success!\n")
	} else {
		fmt.Printf("call failed!\n")
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for {
		replay_type, task_seq, nReduce, file_path := CallForTask()
		switch replay_type {
		case MAP:
			kva := DoMap(file_path, mapf)
			intermediate = append(intermediate, kva...)
			output_file_dir, _ := filepath.Abs("./")
			// todo:错误处理
			DoHashSplit(intermediate, task_seq, nReduce, output_file_dir)
			ReportMapResult(task_seq, output_file_dir)
		case REDUCE:
			fmt.Printf("reduce!")
		case WAIT:
			time.Sleep(2 * time.Second)
		case DONE:
			//todo
			return
		case REQUEST_ERROR:
			//todo, err report
			continue
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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
	// 这里创建了一个rpc客户端，本质上是一个*rpc.Client类型的指针，
	// 它封装了底层的传输（如TCP）以及用于与服务器交互的方法（如Call）
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
