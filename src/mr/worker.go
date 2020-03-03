package mr

import "sort"
import "os"
import "log"
import "net/rpc"
import "hash/fnv"
import "fmt"
import "io/ioutil"
import "encoding/json"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int	   { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i]}
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key}

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
// partition the data into different bucket
// for reduce task
//
func maptask(mapf func(string, string) []KeyValue, task *Task, nReduce int) {
	filename := task.File
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
	mapfiles := make([]ByKey, nReduce)
	for _, kv := range kva{
		reduce_num := ihash(kv.Key)%nReduce
		mapfiles[reduce_num] = append(mapfiles[reduce_num], kv)
	}
	for reduce_num, mapfile := range mapfiles{
		mfile_name := fmt.Sprintf("mr-%v-%v", task.Task_ID, reduce_num)
		mfile, err := os.OpenFile(mfile_name, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0755)
		if err != nil{
			log.Fatalf("open %v failed", mfile_name)
		}
		dec := json.NewEncoder(mfile)
		err = dec.Encode(&mapfile)
		if err != nil{
			log.Fatalf("encode %v failed", mfile_name)
		}
		mfile.Close()
	}
}

func redtask(reducef func(string, []string) string, task *Task){
	nReduce := task.Task_ID
	kva := []KeyValue{}
	file, err := os.Open(".")
	if err != nil{
		log.Fatalf("open the directory failed")
	}
	defer file.Close()
	lists, _ := file.Readdirnames(0)  //0 to read all files and folders
	redfiles := []string{}
	for _, name := range lists{
		if len(name) == 6 && int(name[5])-48 == nReduce && name[0:2] == "mr"{
			redfiles = append(redfiles, name)
		}
	}
	for _, redfile := range redfiles{
		dec_file, err := os.OpenFile(redfile, os.O_RDWR, 0755)
		if err != nil{
			log.Fatalf("open file failed")
		}
		dec := json.NewDecoder(dec_file)
		for{
			var kv []KeyValue
			if err := dec.Decode(&kv); err != nil{
				break
			}
			kva = append(kva, kv...)
		}
	}

	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%v", nReduce)
	ofile, _ := os.Create(oname)

	for i := 0; i < len(kva); i++{
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key{
			j++
		}
		values := []string{}
		for k := i; k < j; k++{
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j-1
	}

	ofile.Close()
}
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	//linb code
	nReduce := CallExample()
	for{
		task, ok := Call_task()
		if ok == false{
			fmt.Printf("can't connect to master\n")
			break
		}
		if(task.Task_type == "map"){
			maptask(mapf, task, nReduce)
			Call_finish(task)
		}
		if(task.Task_type == "reduce"){
			redtask(reducef, task)
			Call_finish(task)
		}
	}
}

// 
// map or reduce finish and return finsh signal
//
func Call_finish(task *Task){
	reply := false
	fmt.Printf("%v_%v is finished!\n", task.Task_type, task.Task_ID)
	call("Master.GetTaskFinish", &task, &reply)
}


//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() int {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	return reply.Y
}

func Call_task() (*Task, bool)  {
	args := Task{}
	reply := Task{}

	//send RPC request to get a task
	ok := call("Master.Assign_task", &args, &reply)

	return &reply, ok
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
