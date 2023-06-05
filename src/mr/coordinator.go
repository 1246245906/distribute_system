package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Condition int

const (
	Start Condition = iota
	Map
	Reduce
	Done
)

type MapTask struct {
	// task_seq   int
	file_path  string
	time_stamp int64
}

type ReduceTask struct {
	input      []string
	time_stamp int64
}

type MapOutput struct {
	task_seq       int
	map_output_dir string
}

type Coordinator struct {
	// Your definitions here.
	cond                  Condition
	map_id                int
	nReduce               int
	unsettled_files       []string
	in_progress_map_tasks map[int]MapTask
	map_outputs           []MapOutput

	// 先设置一个全局锁，优化可以搞一个更细粒度的锁。比如每个切片带一个锁
	mutex sync.Mutex

	// todo: reduce用的变量
	reduce_inputs        [][]string
	running_reduce_tasks map[int]ReduceTask
	reduce_id            int
	reduce_outputs       []string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) get_timeout_filepath() (string, error) {
	for task_seq, task := range c.in_progress_map_tasks {
		if ns2s(time.Now().UnixNano()-task.time_stamp) > 10 {
			// todo: 这里直接 delete 掉会不会导致task.filepath失效呢？先这样，有问题再排查
			delete(c.in_progress_map_tasks, task_seq)
			return task.file_path, nil
		}
	}
	return "nil", errors.New("no time out tasks")
}

func (c *Coordinator) get_timeout_task() ([]string, error) {
	for task_seq, task := range c.running_reduce_tasks {
		if ns2s(time.Now().UnixNano()-task.time_stamp) > 10 {
			delete(c.running_reduce_tasks, task_seq)
			return task.input, nil
		}
	}
	return []string{}, errors.New("no time out tasks")
}

func (c *Coordinator) prepare_reduce_input() {
	for _, mout := range c.map_outputs {
		for i := 0; i < c.nReduce; i += 1 {
			mr_intermedia_path := filepath.Join(mout.map_output_dir, fmt.Sprintf("mr-out-%d-%d", mout.task_seq, i))
			c.reduce_inputs[i] = append(c.reduce_inputs[i], mr_intermedia_path)
		}
	}
}

func (c *Coordinator) AssignTask(args *RequestArgs, reply *Reply) error {
	if args.RequestType == REQ_TASK {
		// 如果 unsettled_files 为空，那么遍历in_progress_map_tasks，从中找一个超时的交给worker
		if c.cond == Map {
			c.mutex.Lock()
			defer c.mutex.Unlock()
			var file_path string
			if len(c.unsettled_files) > 0 {
				// 倒序处理，理论上性能比较好
				file_path = c.unsettled_files[len(c.unsettled_files)-1]
				c.unsettled_files = c.unsettled_files[:len(c.unsettled_files)-1]
			} else {
				var err error
				file_path, err = c.get_timeout_filepath() //todo: 遍历正在进行列表，找出一个超时的。
				if err != nil {
					reply.ReplayType = WAIT
					return nil
				}
			}
			id := c.map_id
			c.map_id += 1
			time_stamp := time.Now().UnixNano()
			task := MapTask{file_path, time_stamp}
			c.in_progress_map_tasks[id] = task
			reply.ReplayType = MAP
			reply.TaskId = id
			reply.NReduce = c.nReduce
			reply.FilePath = file_path
		} else if c.cond == Reduce {
			// todo:reduce
			c.mutex.Lock()
			defer c.mutex.Unlock()
			var reduce_input []string
			if len(c.reduce_inputs) > 0 {
				// 倒序处理，理论上性能比较好
				reduce_input = c.reduce_inputs[len(c.reduce_inputs)-1]
				c.reduce_inputs = c.reduce_inputs[:len(c.reduce_inputs)-1]
			} else {
				var err error
				reduce_input, err = c.get_timeout_task() //todo: 遍历正在进行列表，找出一个超时的。
				if err != nil {
					reply.ReplayType = WAIT
					return nil
				}
			}
			id := c.reduce_id
			c.reduce_id += 1
			time_stamp := time.Now().UnixNano()
			task := ReduceTask{reduce_input, time_stamp}
			c.running_reduce_tasks[id] = task
			reply.ReplayType = REDUCE
			reply.TaskId = id
			reply.NReduce = c.nReduce
			reply.FilePath = ""
			reply.ReduceInput = reduce_input
		} else if c.cond == Done {
			reply.ReplayType = DONE
			return nil
		}
	} else if args.RequestType == REPORT_RESULT {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		if c.cond == Map {
			completed_task_seq := args.TaskSeq
			_, ok := c.in_progress_map_tasks[completed_task_seq]
			if !ok {
				// 说明报告的文件已经被删除了，此时不处理即可
				return nil
			}
			out_file_path := args.Msg
			fmt.Printf("[MAP]: %s is completed, out_path is %s!\n", c.in_progress_map_tasks[completed_task_seq].file_path, out_file_path)
			delete(c.in_progress_map_tasks, completed_task_seq)
			taskout := MapOutput{completed_task_seq, out_file_path}
			c.map_outputs = append(c.map_outputs, taskout)
			// 未处理文件和正在进行的文件没有了，说明map结束了
			if len(c.unsettled_files) == 0 && len(c.in_progress_map_tasks) == 0 {
				// 先将要的数据结构准备好
				c.prepare_reduce_input()
				c.cond = Reduce
			}
		} else if c.cond == Reduce {
			completed_task_seq := args.TaskSeq
			_, ok := c.running_reduce_tasks[completed_task_seq]
			if !ok {
				return nil
			}
			delete(c.running_reduce_tasks, completed_task_seq)
			out_file_path := args.Msg
			c.reduce_outputs = append(c.reduce_outputs, out_file_path)
			if len(c.reduce_inputs) == 0 && len(c.running_reduce_tasks) == 0 {
				// 收集reduce所有输出，重新命名
				c.cond = Done
			}
		}
	} else {
		reply.ReplayType = REQUEST_ERROR
		// todo: 打印错误log
	}
	return nil
}

func (c *Coordinator) init(files []string, nReduce int) {
	c.cond = Start
	c.map_id = 0
	// 这里直接操作files数组，不需要深拷贝
	c.unsettled_files = files
	c.in_progress_map_tasks = make(map[int]MapTask)
	c.map_outputs = []MapOutput{}
	c.nReduce = nReduce

	// todo: reduce用的变量初始化
	c.reduce_inputs = make([][]string, nReduce)
	c.running_reduce_tasks = make(map[int]ReduceTask)
	c.reduce_id = 0
	c.reduce_outputs = []string{}

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
	ret := false
	if c.cond == Done {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.init(files, nReduce)

	c.cond = Map

	c.server()
	return &c
}
