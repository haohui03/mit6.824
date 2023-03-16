package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// uncomment to send the Example RPC to the master.
	// CallExample()
	var reply AssignTaskReply
	var args AssignTaskArgs
	args.Pid = os.Getpid()
	args.State = "free"
	ch := make(chan string)
	change := make(chan string)
	var argLock sync.Mutex
	go func() {
		for {
			argLock.Lock()
			log.Println(os.Getpid(), args.State)
			argLock.Unlock()
			time.Sleep(1 * time.Second)
		}
	}()

	go func() {
		tick := time.NewTicker(1 * time.Second)
		for {
			select {
			case state := <-change:
				//log.Printf("%d set to %s\n", args.Pid, state)
				argLock.Lock()
				args.State = state
				argLock.Unlock()
			case <-tick.C:
				argLock.Lock()
				err := call("Master.Assign", &args, &reply)
				if !err {
					os.Exit(1)
				}
				log.Printf("%d 安排的任务:%s \n", args.Pid, reply.Task)
				switch reply.Task {
				case "setfree":
					args.State = "free"
					argLock.Unlock()
					continue
				case "exit":
					os.Exit(0)
				case "waiting":
					argLock.Unlock()
					continue
				}
				args.State = "working"
				ch <- reply.Task

				argLock.Unlock()
			}
		}
	}()
outer:
	for {
		switch <-ch {
		case "map":
			//log.Printf("%d  %s", args.Pid, reply.Filename)
			file, err := os.Open(reply.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", reply.Filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Filename)
			}
			file.Close()
			mapch := make(chan struct{})
			kva := make([]KeyValue, 1000)
			go func() {
				kva = mapf(reply.Filename, string(content))

				mapch <- struct{}{}

			}()
			select {
			case <-mapch:
			case <-time.After(5 * time.Second):
				log.Printf("pid: %d, quit by fail\n", os.Getpid())
				change <- "fail"
				continue outer
				// os.Exit(2)
			}
			groups := make([][]KeyValue, reply.Nreduce)
			tmpfiles := make([]*os.File, reply.Nreduce)
			for i := 0; i < reply.Nreduce; i++ {
				os.Remove(fmt.Sprintf("mr-%d-%d", reply.Tasknumber, i))
				tmpfiles[i], err = ioutil.TempFile("", "mr-*-")
				if err != nil {
					log.Fatal(err)
				}

				os.Rename(tmpfiles[i].Name(), fmt.Sprintf("mr-%d-%d", reply.Tasknumber, i))
				groups[i] = make([]KeyValue, 0, 20)
				defer func(i int) { os.Remove(fmt.Sprintf("mr-%d-%d", reply.Tasknumber, i)) }(i)
			}

			for _, keyvalue := range kva {
				groups[ihash(keyvalue.Key)%reply.Nreduce] = append(groups[ihash(keyvalue.Key)%reply.Nreduce], keyvalue)
			}
			var wg sync.WaitGroup
			for i := 0; i < reply.Nreduce; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					enc := json.NewEncoder(tmpfiles[i])
					err := enc.Encode(groups[i])
					if err != nil {
						log.Fatal(err)
					}
				}(i)
			}
			wg.Wait()
			change <- "done"
		case "reduce":
			filelist, err := filepath.Glob(fmt.Sprintf("./mr-[0-9]-%d", reply.Tasknumber-1))
			//fmt.Println(filelist)
			if err != nil {
				log.Fatal(err)
			}
			kva := make([]KeyValue, 0, 1000)
			for _, filename := range filelist {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatal(err)
				}
				dec := json.NewDecoder(file)
				for {
					var kv []KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv...)
				}
			}

			sort.Sort(ByKey(kva))
			// fmt.Printf("length of kva:%d\n", len(kva))
			outputfilename := fmt.Sprintf("mr-out-%d", reply.Tasknumber)
			os.Remove(outputfilename)
			ofile, err := os.Create(outputfilename)
			if err != nil {
				log.Fatal(err)
			}
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				var reduceOut string
				reduceCh := make(chan struct{})
				go func() {
					reduceOut = reducef(kva[i].Key, values)
					// select {
					// case
					reduceCh <- struct{}{}
					// default:
					// 	log.Printf("pid: %d  return ", os.Getpid())
					// 	return
					// }
				}()
				select {
				case <-reduceCh:
				case <-time.After(6 * time.Second):
					change <- "fail"
					continue outer
					// os.Exit(2)
				}
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, reduceOut)
				i = j
			}
			fmt.Println("done reduce")
			change <- "done"
			//fmt.Println("reduce done")

		}

	}

}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	//fmt.Printf("reply.Y %v\n", reply.Y)
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

	log.Println(err)
	return false
}
