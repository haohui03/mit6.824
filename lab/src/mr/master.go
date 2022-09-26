package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type WorkerTracker struct {
	State   string
	Tracker chan string
}

type Master struct {
	// Your definitions here.
	Mu        sync.Mutex
	WorkerMap map[int]*WorkerTracker
	Filemap   map[string]int
	Filelist  []string
	reduce    []int
	Nreduce   int
	Wgm       sync.WaitGroup
	Wgr       sync.WaitGroup
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) assignMap(args *AssignTaskArgs, reply *AssignTaskReply) {
	m.Wgm.Add(1)
	reply.Task = "map"
	m.Mu.Lock()
	filename := m.Filelist[len(m.Filelist)-1]
	reply.Filename = filename
	reply.Tasknumber = m.Filemap[filename]
	m.Filelist = m.Filelist[:len(m.Filelist)-1]
	//fmt.Printf("id :%d, tasknumber:%d\n", args.Pid, reply.Tasknumber)
	reply.Nreduce = m.Nreduce
	m.Mu.Unlock()
	go func() {
		defer m.Wgm.Done()
		select {
		case <-time.After(10 * time.Second):
			m.Mu.Lock()
			m.Filelist = append(m.Filelist, filename)
			m.Mu.Unlock()
		case state := <-m.WorkerMap[args.Pid].Tracker:
			if state == "done" {
				break
			} else {
				//failed
				m.Mu.Lock()
				m.Filelist = append(m.Filelist, filename)
				m.Mu.Unlock()
			}
		}
	}()
}

func (m *Master) assignReduce(args *AssignTaskArgs, reply *AssignTaskReply) {
	reply.Task = "reduce"
	reduceNumber := m.reduce[len(m.reduce)-1]
	m.reduce = m.reduce[:len(m.reduce)-1]
	reply.Tasknumber = reduceNumber
	m.Wgr.Add(1)
	go func() {
		defer m.Wgr.Done()
		select {
		case <-time.After(10 * time.Second):
			//fmt.Printf("timeout: %d", reduceNumber)
			m.Mu.Lock()
			m.reduce = append(m.reduce, reduceNumber)
			m.Mu.Unlock()
		case state := <-m.WorkerMap[args.Pid].Tracker:
			if state == "done" {
				break
			} else {
				m.Mu.Lock()
				m.reduce = append(m.reduce, reduceNumber)
				m.Mu.Unlock()
			}
		}
	}()
}

func (m *Master) Assign(args *AssignTaskArgs, reply *AssignTaskReply) error {
	//fmt.Printf("workerid: %d   state: %s\n", args.Pid, args.State)
	//fmt.Println("begin to assign")
	//fmt.Printf("filelist: %v\n", m.Filelist)
	//fmt.Printf("reduce list: %v\n", m.reduce)
	if _, ok := m.WorkerMap[args.Pid]; !ok {
		m.WorkerMap[args.Pid] = &WorkerTracker{State: args.State, Tracker: make(chan string)}
	} else {
		m.WorkerMap[args.Pid].State = args.State
	}
	switch args.State {
	case "free":
		if len(m.Filelist) != 0 { //map
			m.assignMap(args, reply)
		} else {
			if len(m.reduce) != 0 { //reduce
				m.Wgm.Wait() //wait map
				if len(m.Filelist) == 0 {
					m.assignReduce(args, reply)
				} else {
					reply.Task = "waiting"
				}
			} else {
				m.Wgr.Wait()
				if len(m.reduce) == 0 {
					reply.Task = "exit"
				} else {
					reply.Task = "waiting"
				}
			}
		}
	case "done":
		m.Mu.Lock()

		m.WorkerMap[args.Pid].Tracker <- "done"
		//fmt.Println("done")
		//fmt.Printf("filelist: %v\n", m.Filelist)
		//fmt.Printf("reduce list: %v\n", m.reduce)
		m.Mu.Unlock()
		reply.Task = "setfree"
	case "working":
		reply.Task = "waiting"
	case "fail":
		m.Mu.Lock()
		m.WorkerMap[args.Pid].Tracker <- "fail"
		m.Mu.Unlock()
		reply.Task = "setfree"
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
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	if len(m.reduce) == 0 {
		m.Wgr.Wait()
		if len(m.reduce) == 0 {
			time.Sleep(3 * time.Second)
			ret = true
		} else {
			ret = false
		}
	}
	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {

	// Your code here.
	m := Master{}
	m.Filemap = make(map[string]int)
	m.WorkerMap = make(map[int]*WorkerTracker)
	m.Mu.Lock()
	for i := 0; i < len(files); i++ {
		m.Filemap[files[i]] = i + 1
	}
	//fmt.Println(m.Filemap)
	m.Filelist = files
	m.Nreduce = nReduce
	m.reduce = make([]int, nReduce)
	for i := 0; i < nReduce; i++ {
		m.reduce[i] = i + 1
	}
	m.Mu.Unlock()
	// go func (){
	// 	tick:=time.Tick(5*time.Second)
	// 	for{
	// 		<-tick
	// 		m.mu.Lock()
	// 		m.WorkerMap=make(map[int]WorkerTracker)
	// 		m.mu.Unlock()
	// 	}
	// }()
	m.server()
	return &m
}
