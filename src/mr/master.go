package mr

import "os/exec"
import "time"
import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
	exist_tasks []Task
	map_strings []string
	NReduce int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = m.NReduce
	return nil
}



//
//
//deep copy for struct
//
//
func (m *Master) Task_copy(reply *Task, origin *Task ) {
	reply.Task_ID = origin.Task_ID
	reply.Task_type = origin.Task_type
	reply.Exec_time = origin.Exec_time
	reply.File = origin.File
	reply.Assigned = origin.Assigned
	reply.Finished = reply.Finished
	fmt.Printf("Assign %v %v\n", reply.Task_type, reply.Task_ID)
}



//
//
// assign task for worker
//
//
func (m *Master) Assign_task(args *Task, reply *Task) error {
	assign_map := false
	reply.Task_type = "unassigned"
	for i, _ := range m.exist_tasks {
		if m.exist_tasks[i].Task_type == "map"&& m.exist_tasks[i].Assigned == false {
			m.exist_tasks[i].Assigned = true
			m.Task_copy(reply, &m.exist_tasks[i])
			assign_map = true
			break
		}
		if m.exist_tasks[i].Task_type == "map" && m.exist_tasks[i].Finished == false {
			assign_map = true
		}
	}
	if assign_map == false && len(m.exist_tasks)>0 {
		for i := 0; i < len(m.exist_tasks); i++{
			if m.exist_tasks[i].Task_type == "reduce"&&m.exist_tasks[i].Assigned == false   {
				m.exist_tasks[i].Assigned = true
				m.Task_copy(reply, &m.exist_tasks[i])
				break
			}
		}
	}
	return nil
}

//
// get the finish signal
//
func (m *Master) GetTaskFinish(args *Task, reply *bool) error{
	for i := 0; i < len(m.exist_tasks); i++{
		if args.Task_ID == m.exist_tasks[i].Task_ID && args.Task_type == m.exist_tasks[i].Task_type{
			m.exist_tasks[i].Finished = true
		}
	}
	return nil
}





//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
//
//check if all the work finished
//
//
func (m *Master) CheckFinish() bool{
	for _, exist_task := range m.exist_tasks{
		if exist_task.Finished == false {
			return false
		}
	}
	return true
}

//
//check whether task has worked more than 10s
//if true, restart the task
//
func (m *Master) CheckAndRestart() {
	for i := 0; i < len(m.exist_tasks); i++{
		if m.exist_tasks[i].Assigned == true && m.exist_tasks[i].Finished == false && m.exist_tasks[i].Exec_time > 10{
			fmt.Printf("Restart task %v.\n", i)
			m.exist_tasks[i].Assigned = false
			m.exist_tasks[i].Exec_time = 0
			cmd := exec.Command("../mrworker", "../../mrapps/crash.so")
			if err := cmd.Start(); err != nil{
				log.Fatal(err)
			}
		}
	}
}

//
// exec_time of all running tasks plus one
//
func (m *Master) Exectimeplus() {
	for i := 0; i < len(m.exist_tasks); i++{
		if m.exist_tasks[i].Assigned == true && m.exist_tasks[i].Finished == false{
			m.exist_tasks[i].Exec_time ++
		}
	}
}


//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// linbincode
	ret = m.CheckFinish()
	if ret == true{
		fmt.Printf("All the task finish successfully!\n")
	}
	time.Sleep(time.Second)
	m.Exectimeplus()
	m.CheckAndRestart()

	return ret
}


//
// add map task by input filenames
//
func (m *Master)Add_maptask(files []string) {
	for i, file := range files {
		task_single := Task{}
		task_single.Task_ID = i
		task_single.Task_type = "map"
		task_single.File = file
		task_single.Exec_time = 0
		task_single.Finished = false
		task_single.Assigned = false
		m.exist_tasks = append(m.exist_tasks, task_single)
	}
}

func (m *Master)Add_reducetask(nreduce int){
	for i := 0; i < nreduce; i++{
		task_single := Task{}
		task_single.Task_ID = i
		task_single.Task_type = "reduce"
		task_single.Exec_time = 0
		task_single.Finished = false
		task_single.Assigned = false
		m.exist_tasks = append(m.exist_tasks, task_single)
	}
}


//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.NReduce = nReduce
	// Your code here.
	m.Add_maptask(files)
	m.Add_reducetask(nReduce)
	// Your code here
	m.server()
	return &m
}
