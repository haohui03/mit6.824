package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"

	"../labrpc"
)

const (
	LEADER    = iota
	FOLLOWER  = iota
	CANDIDATE = iota
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

type RandomTimer struct {
	Timer *time.Timer
	Rand  *rand.Rand
}

func newRandomTimer(seed int) *RandomTimer {
	source := rand.NewSource(int64(seed))
	rand := rand.New(source)
	ms := 10 + rand.Int63()%450
	randomTimer := &RandomTimer{Timer: time.NewTimer(time.Duration(ms) * time.Millisecond), Rand: rand}
	return randomTimer

}

func (rf *Raft) setElectionTimeout() {
	// if !rf.elecTimer.Timer.Stop() {
	// 	select {
	// 	case <-rf.elecTimer.Timer.C:
	// 	default:
	// 		return
	// 	}
	// }
	ms := 800 + rf.elecTimer.Rand.Int63()%300
	rf.elecTimer.Timer.Reset(time.Duration(ms) * time.Millisecond)
	Debug(dLog, "S%d set election timeout %d and current term: %d, state:%d, logLength: %d", rf.me, ms, rf.currentTerm, rf.state, len(rf.log))
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	log         []*Entry
	currentTerm int
	votedFor    int

	commitIndex int
	lastApplied int

	state          int
	elecTimer      *RandomTimer
	heartBeatTimer *time.Timer
	//for test
	applyCh chan ApplyMsg
	//for Leader
	//Reinitialized after election

	nextIndex  []int //(initialized to leader last log index + 1)
	matchIndex []int //initalized to 0
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) GetMe() int {
	return rf.me
}

func (rf *Raft) GetCurrentTerm() int {
	rf.Lock("Get term")
	defer rf.Unlock("Get Term")
	return rf.currentTerm
}

func (rf *Raft) GetLastApplied() int {
	rf.Lock("getlastApplied")
	defer rf.Unlock("getlastApplied")
	return rf.lastApplied
}

func (rf *Raft) IsLeader() bool {
	rf.Lock("getState")
	defer rf.Unlock("getState")

	return rf.state == LEADER
}

func (rf *Raft) IsLeaderwithoutLock() bool {
	return rf.state == LEADER
}

func (rf *Raft) Lock(s string) {
	Debug(dLock, "S%d want lock %s", rf.me, s)
	rf.mu.Lock()
}

func (rf *Raft) Unlock(s string) {
	Debug(dLock, "S%d release lock %s", rf.me, s)
	rf.mu.Unlock()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.Lock("GetState")
	term = rf.currentTerm
	isleader = rf.state == LEADER
	rf.Unlock("Getstate")
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	// e.Encode(rf.lastApplied)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	Debug(dPersist, "S%d persist", rf.me)

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		Debug(dPersist, "S%d readPersister fail with len(data) %v", rf.me, len(data))
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	Debug(dPersist, "S%d readPersist", rf.me)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var logs []*Entry
	var votedFor int
	// var LastLogIndex int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		Debug(dPersist, "S%d readPersister fail", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
		// rf.lastApplied = LastLogIndex
	}
}

type Entry struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int
	Leader       int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Entry
	LeaderCommit int
	// Your data here (2A, 2B).
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	// Your data here (2A).
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock("AppendEntries")
	state := rf.state
	defer rf.Unlock("AppendEntries")
	rf.setElectionTimeout()
	if rf.currentTerm <= args.Term {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
	} else {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	switch state {
	case LEADER:

		Debug(dError, "S%d leader receive appendEntries  from S%d", rf.me, args.Leader)
	case FOLLOWER, CANDIDATE:
		// if !rf.elecTimer.Timer.Stop() {
		// 	<-rf.elecTimer.Timer.C
		// }
		newTerm := 0
		if len(rf.log) != 0 {
			newTerm = rf.log[len(rf.log)-1].Term
		}
		//if previous match last
		if args.PrevLogIndex == -1 || newTerm == args.PrevLogTerm && len(rf.log)-1 == args.PrevLogIndex {
			//HeartBeat
			if args.Entries == nil {
				Debug(dInfo, "S%d receive heartBeat from %d", rf.me, args.Leader)
				if args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1)
					for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
						Debug(dCommit, "S%d commit %d command %d", rf.me, i, reflect.ValueOf(rf.log[i].Command))
						msg := ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i + 1, CommandTerm: rf.currentTerm}
						rf.applyCh <- msg
					}
					rf.lastApplied = rf.commitIndex
					rf.persist()
				}

			} else {
				if args.PrevLogIndex == -1 {
					// nothing before entries
					rf.log = args.Entries
				} else {
					Debug(dLog, "S%d append log from %d length %d from leader %d entris %v", rf.me, len(rf.log), len(args.Entries), args.Leader, SEntries(args.Entries))
					rf.log = append(rf.log, args.Entries...)
					// rf.persist()
				}
			}
			reply.Success = true
			return
		}
		//do not match but heartBeat
		if args.Entries == nil {
			Debug(dInfo, "S%d receive heartBeat from %d", rf.me, args.Leader)
			return
		}
		//do not match
		conflictIndex := rf.searchEntryIndex(args.PrevLogTerm, args.PrevLogIndex)
		reply.ConflictIndex = conflictIndex
		rf.log = rf.log[:reply.ConflictIndex+1]
		Debug(dLog2, "S%d refuse to apeend args: %v %v from %d with conflictIndex %d ", rf.me, args, SEntries(args.Entries), args.Leader, reply.ConflictIndex)
		reply.Success = false
	}
}

// For fast back tracing
func (rf *Raft) searchEntryIndex(term int, index int) int {
	Debug(dInfo, "S%d search term %d index %d", rf.me, term, index)

	conflictIndex := len(rf.log) - 1
	conflictTerm := 0
	if conflictIndex < index {
		return conflictIndex
	}
	conflictIndex = index
	conflictTerm = rf.log[conflictIndex].Term
	for ; conflictIndex > 0; conflictIndex-- {
		if rf.log[conflictIndex].Term == term {
			return conflictIndex
		} else {
			if rf.log[conflictIndex].Term == conflictTerm {
				continue
			} else {
				return conflictIndex

			}
		}
	}
	return conflictIndex
}

func (rf *Raft) sendAppendEntriesRPC(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		if !reply.Success {
			rf.Lock("receive appendEntries and change term")
			if rf.currentTerm < reply.Term {
				//obey the rule for all server
				rf.state = FOLLOWER
				rf.currentTerm = reply.Term
				rf.persist()
				rf.setElectionTimeout()
			}
			rf.Unlock("receive appendEntries and change term")
		}
	}
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.Lock("RequestVote")
	defer rf.Unlock("RequestVote")

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		Debug(dVote, "S%d (%d)refuse to vote for %d(%d)", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}
	if args.Term == rf.currentTerm && rf.votedFor != -1 {
		Debug(dInfo, "S%d refuse vote to %d", rf.me, args.CandidateId)
		reply.VoteGranted = false
		return
	}
	term := 0
	if len(rf.log) != 0 {
		term = rf.log[len(rf.log)-1].Term
	}
	if args.LastLogTerm > term || args.LastLogTerm == term && args.LastLogIndex >= len(rf.log)-1 {
		reply.VoteGranted = true
		Debug(dVote, "S%d vote %d", rf.me, args.CandidateId)
		rf.state = FOLLOWER
		rf.votedFor = args.CandidateId
		//in case of a small number of servers which is hard to choose a leader
		rf.setElectionTimeout()
	}
	rf.currentTerm = args.Term
	rf.state = FOLLOWER
	rf.persist()
	Debug(dTerm, "S%d set term to %d", rf.me, rf.currentTerm)
	// Your code here (2A, 2B).
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.Lock("sendRequestVote")
	Debug(dLog2, "S%d receive to S %d %t  with vote %t with args:%v", rf.me, server, ok, reply.VoteGranted, *args)
	rf.Unlock("sendRequestVote")
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.Lock("Start")
	index = len(rf.log) + 1
	term = rf.currentTerm
	isLeader = rf.state == LEADER
	if !isLeader {
		rf.Unlock("start")
		return
	}
	// Your code here (2B).
	entry := &Entry{Term: term, Command: command}
	rf.log = append(rf.log, entry)
	Debug(dLeader, "S%d leader add log %v", rf.me, SEntries([]*Entry{entry}))
	rf.persist()
	rf.Unlock("start")
	return
}

func (rf *Raft) handleAppendEntries(server int) {
outer:
	for {
		rf.Lock("check if need to send append")
		isLeader := rf.state == LEADER

		if len(rf.log)-1 < rf.nextIndex[server] {
			rf.Unlock("check if need to send append")
			time.Sleep(time.Duration(20) * time.Millisecond)
			continue
		}

		rf.Unlock("check if need to send append")
		if !isLeader || rf.killed() {
			return
		}

		//promise to Append one entries to each follower
		rf.Lock("set first appendEntries content")
		prevTerm := 0

		if rf.nextIndex[server] != 0 {
			prevTerm = rf.log[rf.nextIndex[server]-1].Term
		}
		newIndex := len(rf.log) - 1
		args := &AppendEntriesArgs{Term: rf.currentTerm, Leader: rf.me, PrevLogIndex: rf.nextIndex[server] - 1, PrevLogTerm: prevTerm, LeaderCommit: rf.commitIndex, Entries: rf.log[rf.nextIndex[server] : newIndex+1]}
		reply := &AppendEntriesReply{}
		isLeader = rf.state == LEADER
		Debug(dLeader, "S%d send append %v to %d command %v first time", rf.me, *args, server, SEntries(args.Entries))
		rf.Unlock("set first appendEntries content")
		//first AppendEntries
		t := time.Now()
		for isLeader && !rf.killed() && !rf.sendAppendEntriesRPC(server, args, reply) {
			Debug(dLeader, "S%d append %v to %d failed first time with time %v", rf.me, SEntries(args.Entries), server, time.Since(t))
			time.Sleep(time.Duration(40) * time.Millisecond)
			rf.Lock("get state")
			isLeader = rf.state == LEADER
			inCurrentTerm := args.Term == rf.currentTerm
			rf.Unlock("get state")
			if !inCurrentTerm {
				return
			}
		}
		Debug(dInfo, "S%d receive from %dreply %v first time", rf.me, server, *reply)
		if reply.Success {
			rf.Lock("set nextIndex")
			rf.matchIndex[server] = newIndex
			rf.nextIndex[server] = newIndex + 1
			Debug(dLeader, "S%d append %v to %d success first time", rf.me, SEntries(args.Entries), server)
			rf.Unlock("set NextIndex")
			continue outer
		} else {
			rf.Lock("change state by fail first time")
			rf.nextIndex[server] = reply.ConflictIndex + 1
			rf.Unlock("change state by fail first time")
		}
		//failed but contain information,find the corrct conflict index
		for {
			Debug(dInfo, "S%d receive from %dreply %v", rf.me, server, *reply)
			//check first time and others

			rf.Lock("get state")
			newIndex = len(rf.log) - 1
			entries := rf.log[rf.nextIndex[server] : newIndex+1]
			args.Entries = entries
			args.PrevLogIndex = rf.nextIndex[server] - 1
			inCurrentTerm := args.Term == rf.currentTerm
			if !inCurrentTerm {
				rf.Unlock("get state")
				return
			}
			if args.PrevLogIndex != -1 {
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			}
			rf.Unlock("get state")

			reply := &AppendEntriesReply{}
			Debug(dLeader, "S%d send reappend %v to %d command %v", rf.me, *args, server, SEntries(args.Entries))
			if isLeader && !rf.killed() && !rf.sendAppendEntriesRPC(server, args, reply) {
				rf.Lock("get state")
				Debug(dLeader, "S%d reappend %v to %d failed ", rf.me, SEntries(args.Entries), server)
				isLeader = rf.state == LEADER
				rf.Unlock("get state")

			}
			if reply.Success {
				rf.Lock("set nextIndex")
				rf.matchIndex[server] = newIndex
				rf.nextIndex[server] = newIndex + 1
				Debug(dLeader, "S%d append %v to %d success next time", rf.me, SEntries(args.Entries), server)
				rf.Unlock("set NextIndex")
				continue outer
			} else {
				rf.Lock("change state by fail first time")
				rf.nextIndex[server] = reply.ConflictIndex + 1
				rf.Unlock("change state by fail first time")
			}
			if !isLeader || rf.killed() {
				return
			}
			if reply.Success {
				rf.Lock("set nextIndex")
				rf.nextIndex[server] = newIndex + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				rf.Unlock("set Nextindex")
				continue outer
			}
			time.Sleep(time.Duration(50) * time.Millisecond)
		}
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	Debug(dDrop, "S%d is killed", rf.me)
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func (rf *Raft) leaderInit() {
	rf.Lock("init leader")
	Debug(dTest, "S%d leader initaliaze", rf.me)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = -1
	}
	peer := len(rf.peers)
	me := rf.me
	rf.heartBeatTimer = time.NewTimer(time.Millisecond)
	rf.Unlock("init leader")
	//heartBeat
	go func() {
		sendHeartBeat := rf.sendAppendEntriesRPC
		for range rf.heartBeatTimer.C {
			rf.Lock("heartbeat")
			state := rf.state
			rf.Unlock("heartbeat")
			Debug(dInfo, "S%d is state: %d", me, state)
			if state == LEADER && !rf.killed() {
				//get args
				rf.Lock("get heartbeat state")
				prvLogTerm := 0
				if len(rf.log) != 0 {
					prvLogTerm = rf.log[len(rf.log)-1].Term
				}
				args := &AppendEntriesArgs{Term: rf.currentTerm, Leader: rf.me, PrevLogIndex: len(rf.log) - 1, PrevLogTerm: prvLogTerm, LeaderCommit: rf.commitIndex}
				rf.Unlock("get heartbeat state")

				for i := 0; i < len(rf.peers); i++ {
					if i != me {
						Debug(dLeader, "S%d send heartbeat to %d", me, i)
						reply := &AppendEntriesReply{}
						go sendHeartBeat(i, args, reply)
					}
				}
			} else {
				return
			}
			rf.heartBeatTimer.Reset(time.Duration(150) * time.Millisecond)
		}
	}()
	//set go routine for each follow
	for i := 0; i < peer; i++ {
		if i == me {
			continue
		}
		go rf.handleAppendEntries(i)
	}

	go rf.checkCommit()
}

func (rf *Raft) checkCommit() {
	for {
		time.Sleep(time.Duration(30) * time.Millisecond)
		rf.Lock("checkCommit")
		//down or killed
		if rf.killed() || rf.state != LEADER {
			rf.Unlock("checkCommit")
			return
		}

		if rf.lastApplied == len(rf.log)-1 {
			rf.Unlock("checkCommit")
			continue
		}
		N := rf.lastApplied + 1
		for N < len(rf.log) && rf.log[N].Term != rf.currentTerm {
			N++
		}
		if N == len(rf.log) {
			rf.Unlock("checkCommit")
			continue
		}
		count := 0
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= N {
				count++
			}
		}
		if count >= (len(rf.peers)+1)/2-1 {
			N = Min(N, len(rf.log)-1)
			for i := rf.lastApplied + 1; i <= N; i++ {
				Debug(dCommit, "S%d commit %d command %d", rf.me, i, reflect.ValueOf(rf.log[i].Command))
				msg := ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i + 1, CommandTerm: rf.currentTerm}
				rf.applyCh <- msg
			}
			rf.commitIndex = N
			rf.lastApplied = rf.commitIndex
			rf.persist()
		}
		rf.Unlock("checkCommit")
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.Lock("make")
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.log = []*Entry{}
	rf.applyCh = applyCh
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.elecTimer = newRandomTimer(me)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(peers))
	rf.readPersist(persister.ReadRaftState())
	Debug(dLog, "S%d is made with lastapplied %d", rf.me, rf.lastApplied)
	rf.Unlock("make")
	// Your initialization code here (2A, 2B, 2C).
	//start  go rountine to listen election timeout

	// initialize from state persisted before a crash
	<-rf.elecTimer.Timer.C
	go rf.serverInit()
	return rf
}
func (rf *Raft) serverInit() {

	electionFunc := func(i int, ch chan RequestVoteReply) {
		rf.Lock("sendvote")
		lastTerm := 0
		if len(rf.log) != 0 {
			lastTerm = rf.log[len(rf.log)-1].Term
		}
		args := &RequestVoteArgs{Term: rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.log) - 1,
			LastLogTerm:  lastTerm}
		reply := &RequestVoteReply{}
		rf.Unlock("sendVote")
		if !rf.killed() && rf.sendRequestVote(i, args, reply) {
			select {
			case ch <- *reply:
			default:
				return
			}
		}
	}

outer:
	for {
		if rf.killed() {
			return
		}
		rf.Lock("setelectiontimeout")
		rf.setElectionTimeout()
		rf.Unlock("setelectiontimeout")
		replyCh := make(chan RequestVoteReply)
		switch rf.state {
		case FOLLOWER, CANDIDATE:
			<-rf.elecTimer.Timer.C
			rf.setElectionTimeout()
			vote := 1
			rf.Lock("get election state")
			rf.state = CANDIDATE
			peer := len(rf.peers)
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.persist()
			me := rf.me
			Debug(dLog2, "S%d start to request vote with term %d", me, rf.currentTerm)
			rf.Unlock("get election state")
			for i := 0; i < peer; i++ {
				if i != me {
					go electionFunc(i, replyCh)
				}
			}
			for {
				select {
				case <-rf.elecTimer.Timer.C:
					rf.Lock("debug")
					Debug(dTimer, "S%d refresh timer", rf.me)
					rf.Unlock("debug")
					// close(replyCh)
					continue outer
				case reply := <-replyCh:
					if rf.state != CANDIDATE {
						continue
					}
					if reply.VoteGranted {
						vote++
						rf.Lock("debug")
						Debug(dInfo, "S%d receive vote current vote :%d", rf.me, vote)
						rf.Unlock("debug")
						if vote >= (peer+1)/2 {
							// close(replyCh)
							rf.Lock("set leader")
							rf.state = LEADER
							Debug(dLeader, "s%d become leader", rf.me)
							rf.Unlock("set leader")
							rf.leaderInit()
							continue outer
						}
					} else {
						rf.Lock("vote fail")
						if rf.currentTerm < reply.Term {
							rf.currentTerm = reply.Term
							rf.state = FOLLOWER
							rf.votedFor = -1
							rf.persist()
							rf.setElectionTimeout()
							// close(replyCh)
							rf.Unlock("vote fail")
							<-rf.elecTimer.Timer.C
							continue outer
						}
						rf.Unlock("vote fail")
					}
				}

			}

		case LEADER:
			time.Sleep(time.Duration(300) * time.Millisecond)
			continue outer
		}
	}
}
