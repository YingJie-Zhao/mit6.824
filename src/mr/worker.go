package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	os "os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	time.Sleep(5 * time.Second)
	for {
		args := RPCArgs{}
		reply := RPCReply{}
		call("Coordinator.GetTask", &args, &reply)
		switch reply.TaskInfo.TaskType {
		case Map:
			doMap(&reply.TaskInfo, mapf)
			//fmt.Printf("Map task %d finished \n", reply.TaskInfo.TaskID)
		case Reduce:
			doReduce(&reply.TaskInfo, reducef)
			//fmt.Printf("Reduce task %d finished \n", reply.TaskInfo.TaskID)
		case Wait:
			//fmt.Println("Waiting for a task...")
			time.Sleep(time.Second)
			continue
		case Done:
			//fmt.Println("All tasks was done.")
			return
		}
		args.TaskInfo = reply.TaskInfo
		call("Coordinator.TaskDone", &args, &reply)
	}
}

func doMap(taskInfo *Task, mapf func(string, string) []KeyValue) {
	//fmt.Printf("Get a map task ID-[%v] FileName: \"%v\"\n", taskInfo.TaskID, taskInfo.FileName)
	file, err := os.Open(taskInfo.FileName)
	if err != nil {
		fmt.Printf("Failed to open file %v\n", taskInfo.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("Failed to read file %v\n", taskInfo.FileName)
	}
	_ = file.Close()

	//初始化map产生的中间数据
	intermediate := make([][]KeyValue, taskInfo.NReduce)
	for i := 0; i < taskInfo.NReduce; i++ {
		intermediate[i] = make([]KeyValue, 0)
	}

	//将读取的文件进行map
	kva := mapf(taskInfo.FileName, string(content))

	//将map得到的键值对根据特定的hash函数写入到指定的reduce索引
	for _, kv := range kva {
		intermediate[ihash(kv.Key)%taskInfo.NReduce] = append(intermediate[ihash(kv.Key)%taskInfo.NReduce], kv)
	}

	for i := 0; i < taskInfo.NReduce; i++ {
		outputFileName := fmt.Sprintf("mr-%d-%d", taskInfo.TaskID, i)
		tempFile, _ := ioutil.TempFile("./", "tmp_")
		enc := json.NewEncoder(tempFile)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Json encode error: Key-%s,Value-%s", kv.Key, kv.Value)
			}
		}
		_ = tempFile.Close()
		_ = os.Rename(tempFile.Name(), outputFileName)
	}
}

func doReduce(taskInfo *Task, reducef func(string, []string) string) {
	//fmt.Printf("Get a reduce task ID-[%d]\n", taskInfo.TaskID)
	intermediate := make([]KeyValue, 0)
	for i := 0; i < taskInfo.NMap; i++ {
		iName := fmt.Sprintf("mr-%d-%d", i, taskInfo.TaskID)
		iFile, _ := os.Open(iName)
		dec := json.NewDecoder(iFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		_ = iFile.Close()
	}

	//将相同key的数据合并至一块
	sort.Sort(ByKey(intermediate))

	oName := fmt.Sprintf("mr-out-%d", taskInfo.TaskID)
	oFile, _ := ioutil.TempFile("./", "tmp_")

	for i := 0; i < len(intermediate); {
		var values []string
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		_, _ = fmt.Fprintf(oFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	_ = oFile.Close()
	_ = os.Rename(oFile.Name(), oName)
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
