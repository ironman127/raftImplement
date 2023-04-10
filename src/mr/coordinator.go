package mr

import "C"
import (
	"fmt"
	"log"
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
	state   State
	TaskAdr *Task
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
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
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
	return &c
}

func (c *Coordinator) makeMapTasks(files []string) {
	for _, v := range files {
		id := c.generateTaskId()
		task := Task{
			TaskType:  MapTask,
			TaskId:    id,
			ReduceNum: c.ReduceNum,
			FileName:  v,
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
	mu.Unlock()

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
	default:
		{
			reply.TaskType = ExitTask
		}
	}
	return nil
}

func (c *Coordinator) toNextPhse() {
	if c.DistPhase == MapPhase {
		c.DistPhase = AllDone
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
				fmt.Printf("Map task[%d] is finished, already!]\n", args.TaskId)
			}
		}
		break
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
