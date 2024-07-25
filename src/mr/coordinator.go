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

type TaskStatus int

const (
	NotStarted TaskStatus = iota
	InProgress
	Completed
)

type Task struct {
	Status    TaskStatus
	WorkerID  int
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	NReduce     int
	MapTasks    map[string]*Task
	ReduceTasks map[int]*Task
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetMapTask(args *GetMapArgs, reply *GetMapReply) error {
	// sleep for 1 second
	// time.Sleep(time.Second)

	// print the maptasks and reducetasks
	// log.Printf("maptasks: %v\n", c.maptasks)
	// log.Printf("maptasks: %v\n", c.reducetasks)
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range c.MapTasks {
		if v.Status == NotStarted {
			reply.Filename = k
			reply.NReduce = c.NReduce
			v.Status = InProgress
			v.StartTime = time.Now()
			return nil
		}
	}
	return nil
}

func (c *Coordinator) GetReduceTask(args *GetReduceArgs, reply *GetReduceReply) error {
	// print the maptasks and reducetasks
	// log.Printf("maptasks: %v\n", c.maptasks)
	// log.Printf("maptasks: %v\n", c.reducetasks)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, task := range c.MapTasks {
		if task.Status != Completed {
			// log.Printf("map task not completed: %v\n", task)
			reply.wait = true
			// print the task that is not completed
			return nil
		}
	}

	for k, v := range c.ReduceTasks {
		if v.Status == NotStarted {
			reply.ReduceN = k
			reply.Filenames = make([]string, 0)
			for k := range c.MapTasks {
				reply.Filenames = append(reply.Filenames, k)
			}
			v.Status = InProgress
			v.StartTime = time.Now()
			return nil
		}
	}
	return nil
}

func (c *Coordinator) MarkMapTaskDone(args *MarkMapTaskDoneArgs, reply *MarkMapTaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	task, ok := c.MapTasks[args.Filename]
	if !ok {
		return errors.New("no such task")
	}
	task.Status = Completed
	return nil
}

func (c *Coordinator) MarkReduceTaskDone(args *MarkReduceTaskDoneArgs, reply *MarkReduceTaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	task, ok := c.ReduceTasks[args.ReduceN]
	if !ok {
		return errors.New("no such task")
	}
	task.Status = Completed
	return nil
}

func (c *Coordinator) Ping(args *struct{}, reply *struct{}) error {
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

	go func() {
		for {
			now := time.Now()
			c.mu.Lock()
			for _, task := range c.MapTasks {
				if task.Status == InProgress && now.Sub(task.StartTime) > (time.Second*5) {
					task.Status = NotStarted
				}
			}
			for _, task := range c.ReduceTasks {
				if task.Status == InProgress && now.Sub(task.StartTime) > (time.Second*5) {
					task.Status = NotStarted
				}
			}
			c.mu.Unlock()

			time.Sleep(time.Second)
		}
	}()
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here
	// check if all the tasks are done
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, task := range c.MapTasks {
		if task.Status != Completed {
			return false
		}
	}
	for _, task := range c.ReduceTasks {
		if task.Status != Completed {
			return false
		}
	}
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce:     nReduce,
		MapTasks:    make(map[string]*Task),
		ReduceTasks: make(map[int]*Task),
	}

	for _, file := range files {
		c.MapTasks[file] = &Task{Status: NotStarted}
	}
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = &Task{Status: NotStarted}
	}

	c.server()
	return &c
}
