package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "sort"
import "strconv"
import "encoding/json"
import "time"
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	flag :=false
	i :=1
	for i==1{
		task := Task{}
		if(flag){
			task.TaskType=Waiting
			flag=false
		}
		if(GetTask(&task)){	
			switch task.TaskType{
				case Waiting:{
					time.Sleep(time.Second)
					flag=true
				}
				case Map:{
					DoMap(mapf,&task)
					FinishTask(&task)
				}
				case Reduce:{
					DoReduce(reducef,&task)
					FinishTask(&task)
				}
				default:{
					i = 2
				}
					
			}
		}else{
			break
		}
	}
	
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func DoMap(mapf func(string, string) []KeyValue, task *Task){
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	intermediate := []KeyValue{}
	Filename := task.Filename 
	file, err := os.Open(Filename)
	if err != nil {
		log.Fatalf("cannot open %v", Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", Filename)
	}
	file.Close()
	kva := mapf(Filename, string(content))
	intermediate = append(intermediate, kva...)
	

	intermediateMap := make([][]KeyValue,task.NReduce)
	for _,tmp := range intermediate{
		intermediateMap[ihash(tmp.Key)%task.NReduce] = append(intermediateMap[ihash(tmp.Key)%task.NReduce],tmp)
	}
	sort.Sort(ByKey(intermediate))
	i := 0
	for i < task.NReduce{
		oname := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range intermediateMap[i]{
			enc.Encode(&kv)
		}
		i++
		ofile.Close()
	}
	
}

func DoReduce(reducef func(string, []string) string, task *Task){
	i:=0
	intermediate:=[]KeyValue{}
	for i<task.NReduce{
		filepath:=task.Filename+strconv.Itoa(i) + "-" + strconv.Itoa(task.TaskId)
		file,_:=os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
		i++
	}
	sort.Sort(ByKey(intermediate))
	i = 0
	oname := "mr-out-"+strconv.Itoa(task.TaskId)
	ofile, _ := os.Create(oname)
	//dir, _ := os.Getwd()
	//tempFile, _ := ioutil.TempFile(dir, "mr-tmp-*")
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
}


func FinishTask(task *Task){
	reply := ExampleReply{}
	call("Coordinator.FinishTask", task,&reply)
}

func GetTask(reply *Task) bool{
	args := ExampleArgs{}
	if(reply.TaskType==Waiting){
		args.X=Waiting
	}
	ok := call("Coordinator.PostTask", &args, reply)
	return ok
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
