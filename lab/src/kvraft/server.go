package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

// const Debug = 0

// func DPrintf(format string, a ...interface{}) (n int, err error) {
// 	if Debug > 0 {
// 		log.Printf(format, a...)
// 	}
// 	return
// }

const (
	opGet    = "Get"
	opPut    = "PUT"
	opAppend = "APPEND"
	opFresh  = "FRESH"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Name   string
	Client int
	Order  int
	Key    string
	Value  string
	// GetCh  chan GetReply
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	content      map[string][]byte
	maxraftstate int // snapshot if log grows this big
	orders       map[int]int
	ClientCh     map[int]map[int]chan struct{}
	done         chan struct{}
	GetNum       int
	fresh        bool
	// GetCh        map[int]chan GetReply
	// orders       map[uint64]uint64
	// Your definitions here.
}

func (kv *KVServer) Lock(s string) {
	Debug(dLock, "S%d kvserver want lock %s", kv.rf.GetMe(), s)
	kv.mu.Lock()
}

func (kv *KVServer) Unlock(s string) {
	Debug(dLock, "S%d kvserver release lock %s", kv.rf.GetMe(), s)
	kv.mu.Unlock()
}
func (kv *KVServer) Get(args *clientArgs, reply *clientReply) {

	if !kv.rf.IsLeader() || kv.killed() {
		reply.IsLeader = false
		return
	}
	kv.Lock("get")
	reply.IsLeader = true
	if _, ok := kv.orders[args.Me]; !ok {
		kv.orders[args.Me] = args.Order
	} else {
		if args.Order > kv.orders[args.Me] {
			kv.orders[args.Me] = args.Order
		}
	}
	kv.InitClient(args.Me)
	ch := make(chan struct{})
	kv.ClientCh[args.Me][args.Order] = ch
	// kv.orders[args.Me] = args.Order
	op := Op{Name: opGet, Key: args.Args.(GetArgs).Key, Client: args.Me, Order: args.Order}
	kv.GetNum += 1
	kv.Unlock("get")
	kv.rf.Start(op)
	//check if commit
	commitCh := make(chan struct{})

	go func() {
		for {
			if !kv.rf.IsLeader() || kv.killed() {
				reply.IsLeader = false
				kv.Lock("finish get")
				kv.GetNum -= 1
				kv.Unlock("finish get")
				commitCh <- struct{}{}
				return
			}
			time.Sleep(time.Duration(50) * time.Millisecond)
		}
	}()

	go func() {
		<-ch
		if v, ok := kv.content[args.Args.(GetArgs).Key]; ok {
			reply.Reply = GetReply{Err: OK, Value: string(v)}
		} else {
			reply.Reply = GetReply{Err: ErrNoKey, Value: ""}
		}
		kv.Lock("finish get")
		kv.GetNum -= 1
		delete(kv.ClientCh[args.Me], args.Order)
		kv.Unlock("finish get")
		kv.done <- struct{}{}
		commitCh <- struct{}{}
	}()

	<-commitCh
	// Debug(dLeader, "S%d  for get %v err: %v", kv.rf.GetMe(), reply.Reply.(GetReply).Value, reply.Reply.(GetReply).Err)
	// getArgs := args.Args
	// Your code here.
}

func (kv *KVServer) InitClient(client int) {
	if _, ok := kv.ClientCh[client]; !ok {
		kv.ClientCh[client] = make(map[int]chan struct{})
	}
}

func (kv *KVServer) PutAppend(args *clientArgs, reply *clientReply) {

	// Your code here.

	if !kv.rf.IsLeader() || kv.killed() {
		reply.IsLeader = false
		return
	}
	kv.CheckFresh()

	kv.Lock("putappend")
	reply.IsLeader = true
	if _, ok := kv.orders[args.Me]; ok {
		if args.Order <= kv.orders[args.Me] {
			kv.Unlock("putappend")
			return
		}
	}
	//leader
	kv.orders[args.Me] = args.Order
	// }
	key := args.Args.(PutAppendArgs).Key
	value := args.Args.(PutAppendArgs).Value

	// kv.content[key] = []byte(value)
	name := ""
	if args.Args.(PutAppendArgs).Op == "Put" {
		name = opPut
	} else {
		name = opAppend
	}
	op := Op{Name: name, Key: key, Value: value, Client: args.Me, Order: args.Order}
	index, _, _ := kv.rf.Start(op)
	kv.Unlock("putappend")
	//check if commit
	commitCh := make(chan struct{})

	go func() {
		for {
			time.Sleep(time.Duration(20) * time.Millisecond)

			if !kv.rf.IsLeader() || kv.killed() {
				reply.IsLeader = false
				commitCh <- struct{}{}
				return
			}

			if kv.rf.GetLastApplied() >= index-1 {
				putAppendReply := PutAppendReply{Err: OK}
				reply.Reply = putAppendReply
				// fmt.Printf("S%d append %v to %v\n", kv.rf.GetMe(), args.Args.(PutAppendArgs).Key, args.Args.(PutAppendArgs).Value)
				commitCh <- struct{}{}
				return
			}
		}
	}()

	<-commitCh

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.content = make(map[string][]byte)
	kv.orders = make(map[int]int)
	kv.fresh = false
	// kv.GetCh = map[int]chan GetReply{}
	// kv.orders = make(map[uint64]uint64)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.ClientCh = map[int]map[int]chan struct{}{}

	kv.done = make(chan struct{})
	kv.GetNum = 0

	// You may need initialization code here.
	go kv.handleCommit()
	return kv
}

func (kv *KVServer) handleCommit() {
	for {
		if kv.killed() {
			return
		}
		appliedMsg := <-kv.applyCh
		op, _ := appliedMsg.Command.(Op)

		kv.orders[op.Client] = op.Order
		if op.Order > kv.orders[op.Client] {
			kv.orders[op.Client] = op.Order
		}

		switch op.Name {
		case opGet:
			kv.handleGet(op)
		case opPut:
			kv.handlePut(op)
		case opAppend:
			kv.handleAppend(op)
		case opFresh:
			kv.handleFresh(op)
		}
		Debug(dCommit, "S%d apply %v", kv.rf.GetMe(), op.Name)
	}
}

func (kv *KVServer) handleGet(op Op) {
	kv.Lock("handleGet")
	ask := kv.GetNum
	kv.Unlock("handleGet")
	if ask != 0 {
		if _, ok := kv.ClientCh[op.Client][op.Order]; ok {
			kv.ClientCh[op.Client][op.Order] <- struct{}{}
			// fmt.Println(op, "ok")
			<-kv.done
		}
	}
}

func (kv *KVServer) handleAppend(op Op) {
	kv.Lock("handleappend")
	kv.content[op.Key] = append(kv.content[op.Key], []byte(op.Value)...)
	kv.Unlock("handleappend")
}

func (kv *KVServer) handlePut(op Op) {
	kv.Lock("handlePut")
	kv.content[op.Key] = []byte(op.Value)
	kv.Unlock("handlePut")
}

func (kv *KVServer) handleFresh(op Op) {
	fresh := false
	if kv.rf.IsLeaderwithoutLock() {
		fresh = true
	}
	kv.Lock("fresh")
	kv.fresh = fresh
	kv.Unlock("fresh")
}

func (kv *KVServer) CheckFresh() {
	kv.Lock("checkFresh")
	if kv.fresh {
		kv.Unlock("checkFresh")
		return
	}

	op := Op{Name: opFresh, Client: 0, Order: -1, Key: "", Value: ""}
	kv.Unlock("checkFresh")
	kv.rf.Start(op)
	for {
		time.Sleep(time.Duration(30) * time.Millisecond)
		kv.Lock("getFreshState")
		fresh := kv.fresh
		kv.Unlock("getFreshState")
		if fresh {
			return
		}
	}
}
