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

var woker_logger = NewLogger("/Users/zhc/projects/6.5840/src/mr/worker.json")

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func CallForTask() (RepType, int, int, string, []string) {
	args := RequestArgs{REQ_TASK, 0, []string{}}
	reply := Reply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	// todo: error process
	if ok {
		return reply.ReplayType, reply.TaskId, reply.NReduce, reply.FilePath, reply.ReduceInput
	} else {
		woker_logger.warn("call failed!")
	}
	return REQUEST_ERROR, 0, 0, "", []string{}
}

func DoMap(file_path string, mapf func(string, string) []KeyValue) []KeyValue {
	// file_name := filepath.Base(file_path)
	file, err := os.Open(file_path)
	if err != nil {
		woker_logger.Fatalf("cannot open %v %s", file_path, err.Error())
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		woker_logger.Fatalf("cannot read %v", file_path)
	}
	kva := mapf(file_path, string(content))
	woker_logger.info("%s map succ!", file_path)
	return kva
}

func DoReduce(kvs []KeyValue, task_id int, reducef func(string, []string) string) (string, error) {
	sort.Sort(ByKey(kvs))
	ofilepath := fmt.Sprintf("./mr-out-%d", task_id)
	tmpfile, _ := ioutil.TempFile(filepath.Dir(ofilepath), "reduce_*")

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

		fmt.Fprintf(tmpfile, "%v %v\n", kvs[i].Key, output)

		i = j
	}
	// todo: errprocess
	err := os.Rename(tmpfile.Name(), ofilepath)
	if err != nil {
		err2 := os.Remove(tmpfile.Name())
		if err2 != nil {
			return "", err2
		}
		return "", err
	}
	abs_opath, _ := filepath.Abs(ofilepath)
	return abs_opath, nil
}

func DoHashSplit(kvs []KeyValue, X int, nReduce int, file_dir string) ([]string, error) {
	outs := []string{}
	out_X := make([][]KeyValue, nReduce)
	for _, kv := range kvs {
		Y := ihash(kv.Key) % nReduce
		out_X[Y] = append(out_X[Y], kv)
	}

	for Y := 0; Y < nReduce; Y += 1 {
		if len(out_X[Y]) > 0 {
			intermedia_output_file_name := fmt.Sprintf("mr-out-%d-%d", X, Y)
			inter_file_path := filepath.Join(file_dir, intermedia_output_file_name)
			err := WriteKV2File(out_X[Y], inter_file_path)
			if err != nil {
				return outs, err
			}
			outs = append(outs, inter_file_path)
		}
	}
	return outs, nil
}

func ReportMapResult(task_id int, inter_outs []string) {
	args := RequestArgs{REPORT_RESULT, task_id, inter_outs}
	reply := Reply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	// todo: error process
	if ok {
		// fmt.Printf("report success!\n")
	} else {
		woker_logger.warn("call failed!")
	}
}

func ReportReduceResult(task_id int, ofilepath string) {
	args := RequestArgs{REPORT_RESULT, task_id, []string{ofilepath}}
	reply := Reply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		// fmt.Printf("[reduce] report success!\n")
	} else {
		woker_logger.warn("[reduce] report failed!")
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
			inter_outs, err := DoHashSplit(intermediate, task_id, nReduce, output_file_dir)
			if err == nil {
				ReportMapResult(task_id, inter_outs)
			} else {
				woker_logger.warn("task id %d faild, do not report!", task_id)
			}
		case REDUCE:
			woker_logger.info("[reduce] start %d reduce task", task_id)
			reduce_kvs := []KeyValue{}
			for _, ri := range reduce_inputs {
				kvs, err := ReadKVFromFile(ri)
				if err != nil {
					woker_logger.warn("[REDUCE][ERR] file %s read faild!", ri)
				} else {
					reduce_kvs = append(reduce_kvs, kvs...)
				}
			}
			if len(reduce_kvs) <= 0 {
				ReportReduceResult(task_id, "")
				continue Loop
			}
			ofilepath, err := DoReduce(reduce_kvs, task_id, reducef)
			if err != nil {
				fmt.Print(err)
				continue Loop
			}
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
