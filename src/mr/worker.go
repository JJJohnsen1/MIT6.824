package mr

import "os"
import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
import "io/ioutil"
import "strconv"
import "time"
import "sort"
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
	h.Write([]byte(key)) //按字节切片
	return int(h.Sum32() & 0x7fffffff) //保证为正值
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	//my code
	alive := true
	for alive {
		task := GetTask()
		switch task.TaskType {
		case MapTask: {
			DoMapTask(&task, mapf)
			callDone(task)
		}
		case ReduceTask: {
			DoReduceTask(&task, reducef)
			callDone(task)
		}
		case ExitTask: {
			alive = false
		}
		case WaittingTask: {
			time.Sleep(time.Second)
		}
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func GetTask() Task {
    args := TaskArgs{} // 为空
    reply := Task{}
    
    // 调用 call 函数
    ok := call("Coordinator.PullTask", &args, &reply)
    
    // 检查返回值
    if !ok {
        fmt.Printf("call failed!")
    }
    
    return reply
}

//maptask-读入split的文件，并输出中间文件mr-x-y
func DoMapTask(task *Task,mapf func(string, string) []KeyValue) {
	intermediate := []KeyValue{}
	filename := task.FileName
    //从文件中得到kv对
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	//分区
	reduceNum := task.ReduceNum
	HashKv := make([][]KeyValue, reduceNum)
	for _, v := range(intermediate) {
		index := ihash(v.Key) % reduceNum
		HashKv[index] = append(HashKv[index], v)
	} 
	//根据分区放入对应文件
	for i :=0 ; i < reduceNum; i++{
		outfilename := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		outfile, err := os.Create(outfilename)
		if err != nil{
			log.Fatalf("cannot open %v", outfilename)
		}
		enc := json.NewEncoder(outfile)
		for _, kv := range(HashKv[i]) {
			err := enc.Encode(kv)
			if err != nil {
				log.Fatalf("encode failed:", err)
			}
		}
		outfile.Close()
	}
}

func DoReduceTask(task *Task,reducef func(string, []string) string) {
	reduceFileNum := task.TaskId
	intermediate := shuffle(task.FileSlice)
	dir, _ := os.Getwd()
	//tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
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

func callDone(task Task) {
	args := task
	reply := TaskArgs{}
	ok := call("Coordinator.MarkDone", &args, &reply)

	if !ok {
		fmt.Printf("call failed!\n")
	}
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

// 洗牌方法，得到一组排序好的kv数组
func shuffle(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
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

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

