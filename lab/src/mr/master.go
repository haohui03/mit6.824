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

func (m *Master) assignMap(args *AssignTaskArgs, reply *AssignTaskReply, filename string) {
	reply.Task = "map"
	m.Mu.Lock()
	reply.Filename = filename
	reply.Tasknumber = m.Filemap[filename]

	//log.Printf("id :%d, tasknumber:%d\n", args.Pid, reply.Tasknumber)
	reply.Nreduce = m.Nreduce
	m.Mu.Unlock()
	go func() {
		defer m.Wgm.Done()
		select {
		case <-time.After(10 * time.Second):
			m.Mu.Lock()
			m.Filelist = append(m.Filelist, filename)
			delete(m.WorkerMap, args.Pid)
			log.Println("===============")
			log.Printf("delete by map 超时 %d\n", args.Pid)
			m.Mu.Unlock()
		case state := <-m.WorkerMap[args.Pid].Tracker:
			if state == "done" {
				break
			} else {
				//failed
				m.Mu.Lock()
				m.Filelist = append(m.Filelist, filename)
				delete(m.WorkerMap, args.Pid)
				m.Mu.Unlock()
			}
		}
	}()
}

func (m *Master) assignReduce(args *AssignTaskArgs, reply *AssignTaskReply, reduceNumber int) {
	reply.Task = "reduce"
	reply.Tasknumber = reduceNumber
	go func() {
		defer m.Wgr.Done()
		select {
		case <-time.After(10 * time.Second):
			//log.Printf("timeout: %d", reduceNumber)
			m.Mu.Lock()
			m.reduce = append(m.reduce, reduceNumber)
			delete(m.WorkerMap, args.Pid)
			log.Println("===============")
			log.Printf("delete by reduce 超时 %d\n", args.Pid)
			m.Mu.Unlock()
		case state := <-m.WorkerMap[args.Pid].Tracker:
			if state == "done" {
				break
			} else {
				m.Mu.Lock()
				m.reduce = append(m.reduce, reduceNumber)
				delete(m.WorkerMap, args.Pid)
				// delete(m.WorkerMap, args.Pid)
				m.Mu.Unlock()
			}
		}
	}()
}

func (m *Master) Assign(args *AssignTaskArgs, reply *AssignTaskReply) error {
	//log.Printf("workerid: %d   state: %s\n", args.Pid, args.State)
	//log.Println("begin to assign")
	//log.Printf("filelist: %v\n", m.Filelist)
	//log.Printf("reduce list: %v\n", m.reduce)
	m.Mu.Lock()
	if _, ok := m.WorkerMap[args.Pid]; !ok {
		log.Printf("printed by master=== pid:%d  state:%v\n", args.Pid, args.State)
		m.WorkerMap[args.Pid] = &WorkerTracker{State: args.State, Tracker: make(chan string)}
	}
	m.Mu.Unlock()
	switch args.State {
	case "free":
		m.Mu.Lock()
		fileLengh := len(m.Filelist)
		var filename string
		if fileLengh != 0 {
			filename = m.Filelist[len(m.Filelist)-1]
			m.Filelist = m.Filelist[:len(m.Filelist)-1]
			m.Wgm.Add(1)
		}
		m.Mu.Unlock()
		if filename != "" { //map
			m.assignMap(args, reply, filename)
			m.Mu.Lock()
			m.WorkerMap[args.Pid].State = "working map"
			m.Mu.Unlock()
		} else {
			m.Wgm.Wait() //wait map
			m.Mu.Lock()
			if len(m.Filelist) == 0 {
				reduceLengh := len(m.reduce)
				reduceNumber := -1
				if reduceLengh != 0 { //reduce
					reduceNumber = m.reduce[len(m.reduce)-1]
					m.reduce = m.reduce[:len(m.reduce)-1]
					m.Wgr.Add(1)
				}
				m.Mu.Unlock()
				if reduceNumber >= 0 {
					m.assignReduce(args, reply, reduceNumber)
					m.Mu.Lock()
					m.WorkerMap[args.Pid].State = "working reduce"
					m.Mu.Unlock()
				} else {
					m.Wgr.Wait()
					m.Mu.Lock()
					if len(m.reduce) == 0 {
						reply.Task = "exit"
						delete(m.WorkerMap, args.Pid)
						log.Println("===============")
						log.Printf("delete by exit %d\n", args.Pid)
						m.Mu.Unlock()
					} else {
						reply.Task = "setfree"
						m.WorkerMap[args.Pid].State = "free"
						m.Mu.Unlock()
					}
				}

			} else {
				reply.Task = "setfree"
				m.WorkerMap[args.Pid].State = "free"
				m.Mu.Unlock()
			}
		}
	case "done":
		m.Mu.Lock()
		m.WorkerMap[args.Pid].Tracker <- "done"
		// log.Println("done")
		// log.Printf("filelist: %v\n", m.Filelist)
		// log.Printf("reduce list: %v\n", m.reduce)
		// for i, worker := range m.WorkerMap {
		// 	log.Printf("pid:%d state:%s\t", i, worker.State)
		// }
		// log.Println()
		m.WorkerMap[args.Pid].State = "free"
		m.Mu.Unlock()
		reply.Task = "setfree"
	case "working":
		reply.Task = "waiting"
	case "fail":
		m.Mu.Lock()
		log.Printf("pid:%d         failed", args.Pid)
		m.WorkerMap[args.Pid].State = "fail"
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
	m.Mu.Lock()
	reduceLength := len(m.reduce)
	workerLength := len(m.WorkerMap)
	log.Println(reduceLength, workerLength)
	if reduceLength == 0 && workerLength == 0 {
		ret = true
		time.Sleep(3 * time.Second)
	}
	for i, worker := range m.WorkerMap {
		log.Printf("pid:%d state:%s\t\n", i, worker.State)
	}
	log.Println("******************")
	log.Println("call done()")
	// Your code here.

	log.Println()
	log.Printf("filelist: %v\n", m.Filelist)
	log.Printf("reduce list: %v\n", m.reduce)
	m.Mu.Unlock()
	log.Println("******************")
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
	//log.Println(m.Filemap)
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
