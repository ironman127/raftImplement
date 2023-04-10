package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	keepFlag := true
	for keepFlag {
		task := getTask()
		switch task.TaskType {
		case MapTask:
			{
				DoMapTask(mapf, &task)
				callDone()
			}
		case WaittingTask:
			{
				fmt.Println("All tasks are in progress, please wait...\n")
				time.Sleep(time.Second)
			}
		case ExitTask:
			{
				fmt.Println("Task about  ：[", task.TaskId, "] is terminated...\n")
				keepFlag = false
			}
		}

	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func getTask() Task {
	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.PollTask", &args, &reply)
	if ok {
		fmt.Println(reply)
	} else {
		fmt.Println("call failed!\n")
	}
	return reply
}

func DoMapTask(mapf func(string, string) []KeyValue, response *Task) {
	var intermediate []KeyValue
	filename := response.FileName

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("cannot open %v", filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal("cannot read %v", filename)
	}
	file.Close()

	intermediate = mapf(filename, string(content))

	rn := response.ReduceNum

	hasdedKV := make([][]KeyValue, rn)

	for _, kv := range intermediate {
		hasdedKV[ihash(kv.Key)%rn] = append(hasdedKV[ihash(kv.Key)%rn], kv)
	}

	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(response.TaskId) + "-" + strconv.Itoa(i)
		ofile, err := os.Create(oname)
		if err != nil {
			log.Fatal("cannot create file %v", oname)
		}

		enc := json.NewEncoder(ofile)
		for _, kv := range hasdedKV[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}

}

func callDone() Task {

	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.MarkFinished", &args, &reply)

	if ok {
		fmt.Println(reply)
	} else {
		fmt.Println("call failed!\n")
	}
	return reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
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
