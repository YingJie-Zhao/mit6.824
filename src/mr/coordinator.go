package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mutex              sync.Mutex
	mapTaskReady       map[int]Task
	mapTaskProgress    map[int]Task
	reduceTaskReady    map[int]Task
	reduceTaskProgress map[int]Task
}

const (
	Map    = 0
	Reduce = 1
	Done   = 2
	Wait   = 3
)

type Task struct {
	FileName  string
	TaskType  int
	TaskID    int
	NReduce   int
	NMap      int
	timestamp int64
}

func (c *Coordinator) GetTask(args *RPCArgs, reply *RPCReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	//收集超时任务
	c.collectStallTask()

	if len(c.mapTaskReady) > 0 {
		for i, task := range c.mapTaskReady {
			reply.TaskInfo = task
			c.mapTaskProgress[i] = task
			delete(c.mapTaskReady, i)
			//fmt.Printf("Distributed map task %d\n", i)
			return nil
		}
	} else if len(c.mapTaskProgress) > 0 {
		reply.TaskInfo = Task{TaskType: Wait}
	} else if len(c.reduceTaskReady) > 0 {
		for i, task := range c.reduceTaskReady {
			reply.TaskInfo = task
			c.reduceTaskProgress[i] = task
			delete(c.reduceTaskReady, i)
			//fmt.Printf("Distributed reduce task %d\n", i)
			return nil
		}
	} else if len(c.reduceTaskProgress) > 0 {
		reply.TaskInfo = Task{TaskType: Wait}
	} else {
		reply.TaskInfo = Task{TaskType: Done}
	}
	return nil
}

func (c *Coordinator) TaskDone(args *RPCArgs, reply *RPCReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	task := args.TaskInfo
	switch task.TaskType {
	case Map:
		delete(c.mapTaskProgress, task.TaskID)
		//fmt.Printf("Map task %d was done\n", task.TaskID)
	case Reduce:
		delete(c.reduceTaskProgress, task.TaskID)
		//fmt.Printf("Reduce task %d was done\n", task.TaskID)
	}
	return nil
}

func (c *Coordinator) collectStallTask() {
	curTime := time.Now().Unix()
	for i, task := range c.mapTaskProgress {
		// job count test require map task only runs 8 times,so timeout parameters for map could be longer
		if curTime-task.timestamp > 20 {
			delete(c.mapTaskProgress, i)
			task.timestamp = curTime
			c.mapTaskReady[i] = task
			fmt.Printf("Collecting stalled map task ID-%d\n", i)
		}
	}
	for i, task := range c.reduceTaskProgress {
		if curTime-task.timestamp > 15 {
			delete(c.reduceTaskProgress, i)
			task.timestamp = curTime
			c.reduceTaskReady[i] = task
			fmt.Printf("Collecting stalled reduce task ID-%d\n", i)
		}
	}
}

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

func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	ret := false

	// Your code here.
	if len(c.mapTaskProgress) == 0 && len(c.mapTaskReady) == 0 && len(c.reduceTaskProgress) == 0 && len(c.reduceTaskReady) == 0 {
		ret = true
	}
	return ret
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapTaskReady = make(map[int]Task)
	c.mapTaskProgress = make(map[int]Task)
	c.reduceTaskReady = make(map[int]Task)
	c.reduceTaskProgress = make(map[int]Task)

	nMap := len(files)
	for i := 0; i < nMap; i++ {
		c.mapTaskReady[i] = Task{
			FileName:  files[i],
			TaskType:  Map,
			TaskID:    i,
			NReduce:   nReduce,
			NMap:      nMap,
			timestamp: time.Now().Unix(),
		}
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTaskReady[i] = Task{
			FileName:  "",
			TaskType:  Reduce,
			TaskID:    i,
			NReduce:   nReduce,
			NMap:      nMap,
			timestamp: time.Now().Unix(),
		}
	}

	c.server()
	return &c
}
