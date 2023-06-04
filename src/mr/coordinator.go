package mr

import (
	"errors"
	"fmt"
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
	// task_seq   int
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
	nReduce               int
	unsettled_files       []string
	in_progress_map_tasks map[int]MapTasks
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
	for task_seq, task := range c.in_progress_map_tasks {
		if ns2s(time.Now().UnixNano()-task.time_stamp) > 10 {
			// todo: 这里直接 delete 掉会不会导致task.filepath失效呢？先这样，有问题再排查
			delete(c.in_progress_map_tasks, task_seq)
			return task.file_path, nil
		}
	}
	return "nil", errors.New("no time out tasks")
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
					reply.ReportType = WAIT
					return nil
				}
			}
			seq := c.cur_seq
			time_stamp := time.Now().UnixNano()
			task := MapTasks{file_path, time_stamp}
			c.in_progress_map_tasks[seq] = task
			reply.ReportType = MAP
			reply.TaskSeq = c.cur_seq
			c.cur_seq += 1
			reply.NReduce = c.nReduce
			reply.FilePath = file_path
		} else if c.cond == Reduce {
			// todo:reduce
		} else if c.cond == Done {
			reply.ReportType = DONE
		}
	} else if args.RequestType == REPORT_RESULT {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		completed_task_seq := args.TaskSeq
		out_file_path := args.Msg
		fmt.Printf("[MAP]: %s is completed, out_path is %s!\n", c.in_progress_map_tasks[completed_task_seq].file_path, out_file_path)
		delete(c.in_progress_map_tasks, completed_task_seq)
		taskout := TaskOutpus{completed_task_seq, out_file_path}
		c.completed_tasks = append(c.completed_tasks, taskout)
		// 未处理文件和正在进行的文件没有了，说明map结束了
		if len(c.unsettled_files) == 0 && len(c.in_progress_map_tasks) == 0 {
			// todo: reduce
			c.cond = Done
		}
	} else {
		reply.ReportType = REQUEST_ERROR
		// todo: 打印错误log
	}
	return nil
}

func (c *Coordinator) init(files []string, nReduce int) {
	c.cond = Start
	c.cur_seq = 0
	// 这里直接操作files数组，不需要深拷贝
	c.unsettled_files = files
	c.in_progress_map_tasks = make(map[int]MapTasks)
	c.completed_tasks = []TaskOutpus{}
	c.nReduce = nReduce

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
