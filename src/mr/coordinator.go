package mr

import "C"
import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

var mu sync.Mutex

type Coordinator struct {
	// Your definitions here.
	ReduceNum         int
	TaskId            int
	DistPhase         Phase
	TaskChannelReduce chan *Task
	TaskChannelMap    chan *Task
	taskMetaHolder    TaskMetaHolder
	files             []string
}

type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo
}

type TaskMetaInfo struct {
	state     State
	startTime time.Time
	TaskAdr   *Task
}

//
// start a thread that listens for RPCs from worker.go
//

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		ReduceNum:         nReduce,
		DistPhase:         MapPhase,
		TaskChannelMap:    make(chan *Task, len(files)),
		TaskChannelReduce: make(chan *Task, nReduce),
		taskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
	}
	c.makeMapTasks(files)
	// Your code here.
	c.server()
	go c.crashHandler()
	return &c
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

func (c *Coordinator) makeMapTasks(files []string) {
	for _, v := range files {
		id := c.generateTaskId()
		task := Task{
			TaskType:  MapTask,
			TaskId:    id,
			ReduceNum: c.ReduceNum,
			FileNames: append(make([]string, 0), v),
		}

		taskMetaInfo := TaskMetaInfo{
			state:   Waitting,
			TaskAdr: &task,
		}

		c.taskMetaHolder.acceptMeta(&taskMetaInfo)
		fmt.Println("make a map task :", &task)
		c.TaskChannelMap <- &task
	}
}

func (c *Coordinator) generateTaskId() int {
	res := c.TaskId
	c.TaskId++
	return res
}

func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	switch c.DistPhase {
	case MapPhase:
		{
			if len(c.TaskChannelMap) > 0 {
				*reply = *<-c.TaskChannelMap
				if !c.taskMetaHolder.judgeState(reply.TaskId) {
					fmt.Printf("taskid [%d] is running\n", reply.TaskId)
				}
			} else {
				reply.TaskType = WaittingTask
				if c.taskMetaHolder.checkTaskAllDone() {
					c.toNextPhse()
				}
				return nil
			}
		}
	case ReducePhase:
		{
			if len(c.TaskChannelReduce) > 0 {
				*reply = *<-c.TaskChannelReduce
				if !c.taskMetaHolder.judgeState(reply.TaskId) {
					fmt.Printf("taskid [%d] is running\n", reply.TaskId)
				}
			} else {
				reply.TaskType = WaittingTask
				if c.taskMetaHolder.checkTaskAllDone() {
					c.toNextPhse()
				}
				return nil
			}

		}

	case AllDone:
		{
			reply.TaskType = ExitTask
		}
	default:
		panic("The phase is undefined!")
	}
	return nil
}

func (c *Coordinator) toNextPhse() {
	if c.DistPhase == MapPhase {
		c.makeReduceTasks()
		c.DistPhase = ReducePhase
	} else if c.DistPhase == ReducePhase {
		c.DistPhase = AllDone
	}

}

func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.TaskType {
	case MapTask:
		{
			meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
			if ok && meta.state == Working {
				meta.state = Done
				fmt.Printf("Map task[%d] is finished!\n", args.TaskId)
			} else {
				fmt.Printf("Map task[%d] is finished, already!\n", args.TaskId)
			}
		}
	case ReduceTask:
		{
			meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
			if ok && meta.state == Working {
				meta.state = Done
				fmt.Printf("Map task[%d] is finished!\n", args.TaskId)
			} else {
				fmt.Printf("Map task[%d] is finished, already!\n", args.TaskId)
			}
		}
	default:
		panic("The task type undefined!")
	}
	return nil
}

func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()

	if c.DistPhase == AllDone {
		fmt.Println("All tasks are finished, the coordinator will be exit!")
		return true
	}

	// Your code here.
	return false
}

func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReduceNum; i++ {
		id := c.generateTaskId()
		task := Task{
			TaskId:    id,
			TaskType:  ReduceTask,
			FileNames: selectReduceName(i),
		}

		taskInfo := TaskMetaInfo{
			state:   Waitting,
			TaskAdr: &task,
		}
		c.taskMetaHolder.acceptMeta(&taskInfo)

		c.TaskChannelReduce <- &task
	}
}

func (c *Coordinator) crashHandler() {
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.DistPhase == AllDone {
			mu.Unlock()
			break
		}

		for _, v := range c.taskMetaHolder.MetaMap {
			if v.state == Working && time.Since(v.startTime) > 9*time.Second {
				fmt.Printf("the task [%d] is crashed, take [%d]  s\n", v.TaskAdr.TaskId, v.startTime)
				switch v.TaskAdr.TaskType {
				case MapTask:
					v.state = Working
					c.TaskChannelMap <- v.TaskAdr
				case ReduceTask:
					v.state = Waitting
					c.TaskChannelReduce <- v.TaskAdr
				}
			}
		}
		mu.Unlock()
	}

}

func selectReduceName(reduceNum int) []string {
	var s []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, fi.Name())
		}
	}
	return s
}

func (t *TaskMetaHolder) acceptMeta(taskInfo *TaskMetaInfo) bool {
	taskId := taskInfo.TaskAdr.TaskId
	meta, _ := t.MetaMap[taskId]
	if meta != nil {
		fmt.Println("mata contains task which i  ", taskId)
		return false
	} else {
		t.MetaMap[taskId] = taskInfo
	}
	return true
}

func (t *TaskMetaHolder) judgeState(id int) bool {
	taskInfo, ok := t.MetaMap[id]
	if !ok || taskInfo.state != Waitting {
		return false
	}
	taskInfo.state = Working
	taskInfo.startTime = time.Now()
	return true
}

func (t *TaskMetaHolder) checkTaskAllDone() bool {
	mapDoneNum := 0
	mapUnDoneNum := 0
	reduceDoneNum := 0
	reduceUnDoneNum := 0

	for _, v := range t.MetaMap {
		if v.TaskAdr.TaskType == MapTask {
			if v.state == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if v.TaskAdr.TaskType == ReduceTask {
			if v.state == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}

	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceUnDoneNum == 0 && reduceDoneNum == 0) {
		return true
	} else if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
		return true
	}
	return false
}
