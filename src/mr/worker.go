package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
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

func CallForTask() (RepType, int, int, string, []string) {
	args := RequestArgs{REQ_TASK, 0, ""}
	reply := Reply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	// todo: error process
	if ok {
		return reply.ReplayType, reply.TaskId, reply.NReduce, reply.FilePath, reply.ReduceInput
	} else {
		fmt.Printf("call failed!\n")
	}
	return REQUEST_ERROR, 0, 0, "", []string{}
}

func DoMap(file_path string, mapf func(string, string) []KeyValue) []KeyValue {
	// file_name := filepath.Base(file_path)
	file, err := os.Open(file_path)
	if err != nil {
		log.Fatalf("cannot open %v %s", file_path, err.Error())
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", file_path)
	}
	kva := mapf(file_path, string(content))
	fmt.Printf("[Worker][MAP]: %s map succ!\n", file_path)
	return kva
}

func DoReduce(kvs []KeyValue, task_id int, reducef func(string, []string) string) string {
	sort.Sort(ByKey(kvs))
	ofilepath := fmt.Sprintf("./mr-out-%d", task_id)
	ofile, _ := os.Create(ofilepath)

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

		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)

		i = j
	}
	// todo: errprocess
	abs_opath, _ := filepath.Abs(ofilepath)
	return abs_opath
}

func DoHashSplit(kvs []KeyValue, X int, nReduce int, file_dir string) error {
	out_X := make([][]KeyValue, nReduce)
	// var Y int
	for _, kv := range kvs {
		Y := ihash(kv.Key) % nReduce
		out_X[Y] = append(out_X[Y], kv)
	}

	for Y := 0; Y < nReduce; Y += 1 {
		if len(out_X[Y]) > 0 {
			intermedia_output_file_name := fmt.Sprintf("mr-out-%d-%d", X, Y)
			err := WriteKV2File(out_X[Y], filepath.Join(file_dir, intermedia_output_file_name))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func ReportMapResult(task_id int, out_dir string) {
	args := RequestArgs{REPORT_RESULT, task_id, out_dir}
	reply := Reply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	// todo: error process
	if ok {
		// fmt.Printf("report success!\n")
	} else {
		fmt.Printf("call failed!\n")
	}
}

func ReportReduceResult(task_id int, ofilepath string) {
	args := RequestArgs{REPORT_RESULT, task_id, ofilepath}
	reply := Reply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		// fmt.Printf("[reduce] report success!\n")
	} else {
		fmt.Printf("[reduce] report failed!\n")
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// intermediate := []KeyValue{}
Loop:
	for {
		replay_type, task_id, nReduce, file_path, reduce_inputs := CallForTask()
		switch replay_type {
		case MAP:
			intermediate := DoMap(file_path, mapf)
			output_file_dir, _ := filepath.Abs("./")
			// todo:错误处理
			err := DoHashSplit(intermediate, task_id, nReduce, output_file_dir)
			if err == nil {
				ReportMapResult(task_id, output_file_dir)
			} else {
				fmt.Printf("[map][reduce] task id %d faild, do not report!\n", task_id)
			}
		case REDUCE:
			fmt.Printf("[worker][reduce] start %d reduce task\n", task_id)
			reduce_kvs := []KeyValue{}
			for _, ri := range reduce_inputs {
				kvs, err := ReadKVFromFile(ri)
				if err != nil {
					fmt.Printf("[REDUCE][ERR] file %s read faild!\n", ri)
				} else {
					reduce_kvs = append(reduce_kvs, kvs...)
				}
			}
			if len(reduce_kvs) <= 0 {
				ReportReduceResult(task_id, "")
				continue Loop
			}
			ofilepath := DoReduce(reduce_kvs, task_id, reducef)
			for _, ri := range reduce_inputs {
				os.Remove(ri)
			}
			ReportReduceResult(task_id, ofilepath)
		case WAIT:
			time.Sleep(1 * time.Second)
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
