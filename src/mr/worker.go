package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	keepFlag := true
	for keepFlag {
		task := getTask()
		switch task.TaskType {
		case MapTask:
			{
				DoMapTask(mapf, &task)
				callDone(&task)
			}
		case ReduceTask:
			{
				doReduceTask(reducef, &task)
				callDone(&task)
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
		fmt.Println("get task ", reply)
	} else {
		fmt.Println("call failed!\n")
	}
	return reply
}

func DoMapTask(mapf func(string, string) []KeyValue, response *Task) {
	var intermediate []KeyValue
	filename := response.FileNames[0]
	fmt.Println(response.FileNames)

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

func doReduceTask(reducef func(string, []string) string, response *Task) {
	reduceFileNum := response.TaskId
	intermediate := shuffle(response.FileNames)
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create tmp file", err)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	fn := fmt.Sprintf("mr-out-%d", reduceFileNum)
	os.Rename(tempFile.Name(), fn)
}

func shuffle(files []string) []KeyValue {
	var kva []KeyValue
	for _, filePath := range files {
		file, _ := os.Open(filePath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	return kva
}

func callDone(task *Task) Task {

	args := TaskArgs{
		TaskId: task.TaskId,
	}
	reply := Task{}
	ok := call("Coordinator.MarkFinished", &args, &reply)

	if ok {
		fmt.Println("task marks finish ", reply)
	} else {
		fmt.Println("call failed!\n")
	}
	return reply
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
