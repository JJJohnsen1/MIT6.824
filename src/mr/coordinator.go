package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "sync"
import "io/ioutil"
import "strconv"
import "strings"
import "time"
var mu sync.Mutex

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
//(a ByKey)是Bykey的一个方法,Bykey可以进行调用
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


// TaskType 对于下方枚举任务的父类型
type TaskType int

// Phase 对于分配任务阶段的父类型
type Phase int

// State 任务的状态的父类型
type State int

// 枚举任务的类型
const (
	MapTask TaskType = iota
	ReduceTask
	WaittingTask // Waitting任务代表此时为任务都分发完了，但是任务还没完成，阶段未改变
	ExitTask     // exit
)

// 枚举阶段的类型
const (
	MapPhase    Phase = iota // 此阶段在分发MapTask
	ReducePhase              // 此阶段在分发ReduceTask
	AllDone                  // 此阶段已完成
)

// 任务状态类型
const (
	Working State = iota // 此阶段在工作
	Waiting              // 此阶段在等待执行
	Done                 // 此阶段已经做完
)

// TaskMetaInfo 保存任务的元数据
type TaskMetaInfo struct {
	state     State     // 任务的状态
	TaskAdr   *Task     // 传入任务的指针,为的是这个任务从通道中取出来后，还能通过地址标记这个任务已经完成
	StartTime time.Time // 任务的开始时间，为crash做准备
}

// TaskMetaHolder 保存全部任务的元数据
type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo // 通过下标hash快速定位
}

type Coordinator struct {
	// Your definitions here.
	MapChan chan *Task	// Map任务channel
	ReduceChan chan *Task	// Reduce任务channel
	ReduceNum int	// Reduce的数量
	TaskId  int     // task的数量
	Files	[]string	// 文件
	DistPhase         Phase          // 目前整个框架应该处于什么任务阶段
	taskMetaHolder    TaskMetaHolder // 存着task
}

type Task struct{
	TaskType TaskType    // 任务类型-map/reduce/exit/Waitting
	FileName string // 任务文件名
	FileSlice []string 
	TaskId	int		// 任务ID，生成中间文件要用
	ReduceNum int	// Reduce的数量,生成中间文件要用
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:             files,
		ReduceNum:        nReduce,
		DistPhase:         MapPhase,
		MapChan:    make(chan *Task, len(files)),
		ReduceChan: make(chan *Task, nReduce),
		taskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce), // 任务的总数应该是files + Reducer的数量
		},
	}
	// Your code here.
	// 制造Map任务
	c.MakeMapTasks(files)

	c.server()

	go c.CrashHandler()
	return &c
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	mu.Lock()
	defer mu.Unlock()
	if c.DistPhase == AllDone {
		fmt.Printf("All tasks are finished,the coordinator will be exit! !")
		return true
	} else {
		return false
	}

}

func (c *Coordinator) PullTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	// 判断任务类型存任务
	switch c.DistPhase {
	case MapPhase:
		{
			if len(c.MapChan) > 0 {
				*reply = *<-c.MapChan
				if !c.taskMetaHolder.judgeState(reply.TaskId) {
					fmt.Printf("Map-askid[ %d ] is working\n", reply.TaskId)
				}
			} else {
				reply.TaskType = WaittingTask // 如果map任务被分发完了但是又没完成，此时就将任务设为Waitting
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case ReducePhase:
		{
			if len(c.ReduceChan) > 0 {
				*reply = *<-c.ReduceChan
				if !c.taskMetaHolder.judgeState(reply.TaskId) {
					fmt.Printf("Reduce-taskid[ %d ] is running\n", reply.TaskId)
				}
			} else {
				reply.TaskType = WaittingTask // 如果Reduce任务被分发完了但是又没完成，此时就将任务设为Waitting
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case AllDone:
		{
			reply.TaskType = ExitTask
		}
	default:
		{
			panic("The phase is undefined!")
		}

	}
	return nil
}

// 判断给定任务是否在工作，并修正其目前任务信息状态
func (t *TaskMetaHolder) judgeState(taskId int) bool {
	taskInfo, ok := t.MetaMap[taskId]
	if !ok || taskInfo.state != Waiting {
		return false
	}
	taskInfo.state = Working
	taskInfo.StartTime = time.Now()
	return true
}

// 检查多少个任务做了包括（map、reduce）,
func (t *TaskMetaHolder) checkTaskDone() bool {

	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)

	// 遍历储存task信息的map
	for _, v := range t.MetaMap {
		// 首先判断任务的类型
		if v.TaskAdr.TaskType == MapTask {
			// 判断任务是否完成,下同
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
	// 如果某一个map或者reduce全部做完了，代表需要切换下一阶段，返回true
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
	} else {
		if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
			return true
		}
	}

	return false

}

func (c *Coordinator) toNextPhase() {
	if c.DistPhase == MapPhase {
		// todo
		c.TaskId = 0
		c.taskMetaHolder.MetaMap =  make(map[int]*TaskMetaInfo, c.ReduceNum) // 任务的总数应该是files + Reducer的数量
		c.makeReduceTasks()
		c.DistPhase = ReducePhase
	} else if c.DistPhase == ReducePhase {
		c.DistPhase = AllDone
	}
}

// 将接受taskMetaInfo储存进MetaHolder里
func (t *TaskMetaHolder) acceptMeta(TaskInfo *TaskMetaInfo) bool {
	taskId := TaskInfo.TaskAdr.TaskId
	meta, _ := t.MetaMap[taskId]
	if meta != nil {
		fmt.Println("meta contains task which id = ", taskId)
		return false
	} else {
		t.MetaMap[taskId] = TaskInfo
	}
	return true
}

func (c *Coordinator) generateTaskId() int {
	res := c.TaskId
	c.TaskId++
	return res
}

// 将生成的任务放入map管道
func (c *Coordinator) MakeMapTasks(files []string) {
	for _, v := range(files) {
		// 生成任务
		id := c.generateTaskId()
		task := Task {
			TaskType: MapTask,
			FileName: v,
			TaskId: id,
			ReduceNum: c.ReduceNum,
		}
		
	    // 保存任务的初始状态
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting, // 任务等待被执行
			TaskAdr: &task,   // 保存任务的地址
		}

		c.taskMetaHolder.acceptMeta(&taskMetaInfo)

		c.MapChan <- &task	// 写入通道
		//fmt.Println(v, "写入成功, Map Task id为", id)
	}
}

func selectReduceName(reduceNum int) []string {
	var s []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		// 匹配对应的reduce文件
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, fi.Name())
		}
	}
	return s
}


func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReduceNum; i++ {
		id := c.generateTaskId()
		task := Task{
			TaskId: id,
			TaskType:  ReduceTask,
			FileSlice: selectReduceName(i),
			ReduceNum: c.ReduceNum,
		}

		// 保存任务的初始状态
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting, // 任务等待被执行
			TaskAdr: &task,   // 保存任务的地址
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)

		c.ReduceChan <- &task
		//fmt.Println("写入成功, Reduce Task id为", id)
	}
}

func (c *Coordinator) MarkDone(args *Task, reply *TaskArgs) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.TaskType {
	case MapTask:
		meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
		//prevent a duplicated work which returned from another worker
		if ok && meta.state == Working {
			meta.state = Done
			//fmt.Printf("Map task Id[%d] is finished.", args.TaskId)
		}
	case ReduceTask:
		meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
		//prevent a duplicated work which returned from another worker
		if ok && meta.state == Working {
			meta.state = Done
			//fmt.Printf("Reduce task Id[%d] is finished.", args.TaskId)
		}
	default:
		panic("The task type undefined ! ! !")
	}
	return nil
}


func (c *Coordinator) CrashHandler() {
	for {
		time.Sleep(time.Second * 2) //防止一直获取锁，造成pulltask获取不到锁
		mu.Lock()
		if c.DistPhase == AllDone {
			mu.Unlock()
			break
		}

		for _, v := range c.taskMetaHolder.MetaMap {
			if v.state == Working && time.Since(v.StartTime) > 9*time.Second {
				//fmt.Printf("the task[ %d ] is crash,take [%d] s\n", v.TaskAdr.TaskId, time.Since(v.StartTime))

				switch v.TaskAdr.TaskType {
				case MapTask:
					c.MapChan <- v.TaskAdr
					v.state = Waiting
				case ReduceTask:
					c.ReduceChan <- v.TaskAdr
					v.state = Waiting

				}
			}
		}
		mu.Unlock()
	}

}
