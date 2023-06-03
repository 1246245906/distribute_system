package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
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

type MapTasks struct {
	task_seq   int
	file_path  string
	time_stamp int64
}

type TaskOutpus struct {
	task_seq     int
	out_put_path string
}

type Coordinator struct {
	// Your definitions here.
	cond                  Condition
	cur_seq               int
	unsettled_files       []string
	in_progress_map_tasks []MapTasks
	completed_tasks       []TaskOutpus

	// 先设置一个全局锁，优化可以搞一个更细粒度的锁。比如每个切片带一个锁
	mutex sync.Mutex

	// todo: reduce用的变量
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
	for _, task := range c.in_progress_map_tasks {
		if ns2s(time.Now().UnixNano()-task.time_stamp) > 10 {
			// todo: 列表中删除该任务
			return task.file_path, nil
		}
	}
	return "nil", errors.New("no time out tasks")
}

func (c *Coordinator) AssignTask(args *RequestArgs, reply *Reply) error {
	if args.req_type == REQ_TASK {
		// 如果unsettled_files为空，那么遍历in_progress_map_tasks，从中找一个超时的交给worker
		// 先用mutex吧，之后优化可以使用原子锁实现个TAS
		if c.cond == Map {
			c.mutex.Lock()
			defer c.mutex.Unlock()
			var file_path string
			if len(c.unsettled_files) > 0 {
				file_path = c.unsettled_files[0]
			} else {
				var err error
				file_path, err = c.get_timeout_filepath() //todo: 遍历正在进行列表，找出一个超时的。
				if err != nil {
					reply.rep_type = WAIT
					return nil
				}
			}
			seq := c.cur_seq
			time_stamp := time.Now().UnixNano()
			task := MapTasks{seq, file_path, time_stamp}
			c.in_progress_map_tasks = append(c.in_progress_map_tasks, task)
			reply.rep_type = MAP
			reply.task_seq = c.cur_seq
			reply.file_path = file_path
		} else if c.cond == Reduce {
			// todo:
		} else if c.cond == Done {
			reply.rep_type = DONE
			return nil
		}
	} else if args.req_type == REPORT_RESULT {
		// todo: 从in_progress_map——tasks中将删除该任务，同时completed_tasks中添加相应任务
	} else {
		reply.rep_type = REQUEST_ERROR
		// todo: 打印错误log
	}
	return nil
}

func (c *Coordinator) init(files []string, nReduce int) {
	c.cond = Start
	c.cur_seq = 0
	// 这里直接操作files数组，不需要深拷贝
	c.unsettled_files = files
	c.in_progress_map_tasks = []MapTasks{}
	c.completed_tasks = []TaskOutpus{}

	// todo: reduce用的变量初始化
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

	// Your code here.

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
