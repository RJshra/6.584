package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
//import "fmt"

type Coordinator struct {
	// Your definitions here.
	State int
	MapChan chan *Task
	MapTasks []*Task
	ReduceChan chan *Task
	ReduceTasks []*Task
	RemainTasks int
}

func (c *Coordinator) PostTask(args *ExampleArgs,reply *Task) error{
	mutex.Lock()
	defer mutex.Unlock()
	if(c.State==Map){
		if len(c.MapChan) > 0{
			*reply= *<-c.MapChan
			c.MapTasks[reply.TaskId].Start=time.Now()
		}else{
			if(args.X!=Waiting){
				reply.TaskType=Waiting
			}else{
				i:=0
				for i< len(c.MapTasks){
					if (c.MapTasks[i].Finished==false&&time.Since(c.MapTasks[i].Start)>time.Second*15){
						*reply=*c.MapTasks[i]
						c.MapTasks[i].Start=time.Now()
						break
					}
					i++
				}
				
			}
			
		}
	}else if(c.State==Reduce){
		if len(c.ReduceChan)>0{
			*reply= *<-c.ReduceChan
			c.ReduceTasks[reply.TaskId].Start=time.Now()
		}else{
			if(args.X!=Waiting){
				reply.TaskType=Waiting
			}else{
				i:=0
				for i< len(c.ReduceTasks){
					if (c.ReduceTasks[i].Finished==false&&time.Since(c.ReduceTasks[i].Start)>time.Second*15){
						*reply=*c.ReduceTasks[i]
						c.ReduceTasks[i].Start=time.Now()
						break
					}
					i++
				}
				
			}
		}
	}else{
		reply.TaskType=0
	}
	return nil
}

func (c *Coordinator) FinishTask(task *Task,reply *ExampleReply) error{
	if(task.TaskType==Map){
		mutex.Lock()
		if(c.MapTasks[task.TaskId].Finished==false){
			c.MapTasks[task.TaskId].Finished=true
			c.RemainTasks--
		}
		mutex.Unlock()
		//fmt.Println(c.MapTasks[task.TaskId])
	}else if(task.TaskType==Reduce){
		mutex.Lock()
		if(c.ReduceTasks[task.TaskId].Finished==false){
			c.ReduceTasks[task.TaskId].Finished=true
			c.RemainTasks--
		}
		mutex.Unlock()
	}
	
	if(c.Check(task.NReduce)){
		c.State=Waiting
	}
	
	return nil
}

func (c *Coordinator) Check(Nreduce int)  bool{
	if(c.RemainTasks==0){
		if(len(c.ReduceTasks)==0){
			c.CreateReduce(Nreduce)
			return false
		}
		return true
	}else{
		
	}
	return false
}

func (c *Coordinator) CreateReduce(Nreduce int){
	i :=0
	for i<Nreduce{
		task :=Task{
			TaskType :Reduce,
			Filename :"mr-",
			NReduce :len(c.MapTasks),
			TaskId :i,
			Finished :false,
			Start:time.Now(),
		}
		c.ReduceChan <- &task
		c.ReduceTasks=append(c.ReduceTasks,&task)
		i++
	}
	c.State=Reduce
	c.RemainTasks=Nreduce
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	//reply.Y = args.X + 1
	return nil
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
func (c *Coordinator) Done() bool {
	ret := (c.State==Waiting)
	// Your code here.
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, NReduce int) *Coordinator {
	c := Coordinator{
		State : Map,
		MapChan : make(chan *Task,len(files)),
		ReduceChan : make(chan *Task,NReduce),
		MapTasks: make([]*Task,len(files)),
		ReduceTasks: make([]*Task,0),
		RemainTasks:len(files),
	}
	i :=0
	for _,file := range files{
		task := Task{
			TaskType: Map,
			Filename: file,
			NReduce: NReduce,
			TaskId: i,
			Finished:false,
			Start:time.Now(),
		}
		c.MapChan <- &task
		//c.MapTasks = append(c.MapTasks,&task)
		c.MapTasks[i]=&task
		i++
	}
	c.server()
	return &c
}
