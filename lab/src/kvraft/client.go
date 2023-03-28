package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"

	"../labrpc"
)

var clientMark int64

type Clerk struct {
	mu      *sync.Mutex
	servers []*labrpc.ClientEnd
	leader  int
	order   int
	me      int
	// You will have to modify this struct.
}

type clientReply struct {
	IsLeader bool
	Reply    interface{}
}

type clientArgs struct {
	Me    int
	Order int
	Args  interface{}
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.mu = &sync.Mutex{}
	ck.servers = servers
	ck.leader = 0
	ck.order = 0
	ck.me = int(atomic.AddInt64(&clientMark, 1))
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	args := clientArgs{Me: ck.me, Order: ck.order, Args: GetArgs{Key: key}}
	clientReply := clientReply{Reply: GetReply{}}
	ck.order++
	ck.mu.Unlock()
	ck.findLeaderCommand("KVServer.Get", &args, &clientReply)
	Debug(dLog, "get err:%v\n", clientReply.Reply.(GetReply).Err)
	if clientReply.Reply.(GetReply).Err != OK {
		Debug(dClient, "client get %v false err: %v", args.Args.(GetArgs).Key, clientReply.Reply.(GetReply).Err)
	}
	// You will have to modify this function.
	return clientReply.Reply.(GetReply).Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	args := clientArgs{Me: ck.me, Order: ck.order, Args: PutAppendArgs{Key: key, Value: value, Op: op}}
	clientReply := clientReply{Reply: PutAppendReply{}}
	ck.order++
	ck.mu.Unlock()
	ck.findLeaderCommand("KVServer.PutAppend", &args, &clientReply)
	if clientReply.Reply.(PutAppendReply).Err != OK {
		Debug(dClient, "client get %v false err: %v", args.Args.(PutAppendArgs).Key, clientReply.Reply.(PutAppendReply).Err)
	}
	// You will have to modify this function.
	// You will have to modify this function.
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

//args and reply should be pointer
func (ck *Clerk) findLeaderCommand(funcName string, args *clientArgs, reply *clientReply) {
	ok := false
	// t := reflect.TypeOf((*reply).Reply)
	// fmt.Println(reflect.TypeOf(reply.Reply))
	for {
		// fmt.Println(ck.me, " call", args, "to ", ck.leader)
		ok = ck.servers[ck.leader].Call(funcName, args, reply)
		if ok {
			if reply.IsLeader {
				// fmt.Println(ck.me, " call", args, "to ", ck.leader, "success")
				return
			}

			// reply = &clientReply{Reply: reflect.New(t).Elem().Interface()}
			// fmt.Println(reflect.TypeOf(reply.Reply))
		}
		ck.mu.Lock()
		// ck.order++
		// args.Order = ck.order
		ck.leader = (ck.leader + 1) % len(ck.servers)
		ck.mu.Unlock()
		// time.Sleep(time.Duration(15) * time.Millisecond)
		continue
		// v := ck.Get(reflect.ValueOf(args.Args).FieldByName("Key").String())
		// fmt.Println(args, "fail but get", v)
	}
}
