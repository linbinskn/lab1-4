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

import "sync"
import "sync/atomic"
import "../labrpc"
import "math/rand"
import "time"
import "fmt"
// import "bytes"
// import "../labgob"


const follower = "follower"
const leader = "leader"
const candidate = "candidate"


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
}

type Log struct {
	Command	     interface{}
	Term         int
}
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	voteFor     int
	serverNum   int
	waitTime    int
	electTime   int
	state       string
	// logs
	logs	    []Log
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCh *chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.state == leader)
	return term, isleader
}

func min(a, b int) int {
	if(a < b){
		return a
	}
	return b
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
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
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
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term	        int
	CandidateId     int
	LastlogIndex	int
	LastlogTerm	int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	CurrentTerm	int
	VoteGrated	bool
}

type AppendEntries struct {
	Term	      int  //leader's
	LeaderId      int
	Pervlogterm   int
	Pervlogindex  int
	Entries	      []Log
	LeaderCommit  int
}

type AppendEntriesReply struct {
	Term	      int
	Success       bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("%v requestvote term %v state %v \n", rf.me, rf.currentTerm, rf.state)
	if args.Term < rf.currentTerm {
		reply.VoteGrated = false
		reply.CurrentTerm = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm{
		rf.state = follower
		rf.voteFor = -1
		rf.waitTime = 0
		rf.currentTerm = args.Term
	}
	voteflag := (rf.voteFor == -1 || rf.voteFor == args.CandidateId)
	if voteflag && args.LastlogTerm >= rf.logs[len(rf.logs)-1].Term{	//Only Term is enough
		rf.waitTime = 0
		rf.currentTerm = args.Term        //  don't be hurry
		rf.voteFor = args.CandidateId
		reply.CurrentTerm = rf.currentTerm
		reply.VoteGrated = true
		fmt.Printf("%v vote the %v\n", rf.me, args.CandidateId)
	}else if(rf.currentTerm < args.Term){
		rf.currentTerm = args.Term
		rf.state = follower
		//rf.waitTime = 0
		//rf.voteFor = args.CandidateId        //attention
	}
	return
}


func (rf *Raft) ReceiveAppendEntries(args *AppendEntries, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("%v receive appendentries from %v\n", rf.me, args.LeaderId)
	var match bool
	if args.Pervlogindex >= len(rf.logs) {
		match = false
	}else {
		match = (rf.logs[args.Pervlogindex].Term == args.Pervlogterm)
	}
	if args.Term < rf.currentTerm {
		fmt.Printf("me:%v, args.Term:%v, currentTerm:%v, match:%v\n", rf.me, args.Term, rf.currentTerm, match)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.state = follower
	rf.voteFor = -1
	rf.waitTime = 0
	if match == false {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.logs = append(rf.logs[:args.Pervlogindex+1], args.Entries...)
	//rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
	reply.Term = rf.currentTerm
	reply.Success = true
	return
}

func (rf *Raft) ReceiveHeartbeat(args *AppendEntries, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = true
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}else{
		rf.currentTerm = args.Term
		commitIndex := rf.commitIndex
		rf.state = follower
		rf.waitTime = 0
		rf.voteFor = -1
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
		if commitIndex < rf.commitIndex {
			for i := commitIndex+1; i <= rf.commitIndex; i++ {
				applymsg := ApplyMsg{true, rf.logs[i].Command, i}
				*rf.applyCh <- applymsg
				rf.lastApplied = i
			}
		}
	}
	return
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
	return ok
}

func (rf *Raft) sendHeartbeat(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.ReceiveHeartbeat", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.ReceiveAppendEntries", args, reply)
	return ok
}

func (rf *Raft) SendRequestVote(serverid int) RequestVoteReply{
	args := RequestVoteArgs{}
	reply := RequestVoteReply{}
	reply.VoteGrated = false
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastlogIndex = len(rf.logs)-1
	args.LastlogTerm = rf.logs[len(rf.logs)-1].Term
	count := 0
	for{
		if ok := rf.sendRequestVote(serverid, &args, &reply);ok{
			break
		}
		break
		//fmt.Printf("%v requestvote failed.\n", serverid)
		count++
		time.Sleep(10 * time.Millisecond)
		if count > 1{
			break
		}
	}
	return reply
}

func (rf *Raft) SendHeartbeat(server int) AppendEntriesReply{
	args := AppendEntries{}
	reply := AppendEntriesReply{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	count := 0
	for{
		if ok := rf.sendHeartbeat(server, &args, &reply); ok{
			break
		}
		break
		count++
		time.Sleep(10 * time.Millisecond)
		if(count > 1){
			break
		}
	}
	return reply
}


func (rf *Raft) AppendEntries(server int) AppendEntriesReply{
	reply := AppendEntriesReply{}
	for{
		args := AppendEntries{}
		reply.Success = false
		rf.mu.Lock()
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.Entries = rf.logs[rf.nextIndex[server]:]
		args.Pervlogindex = rf.nextIndex[server]-1
		args.Pervlogterm = rf.logs[args.Pervlogindex].Term
		args.LeaderCommit = rf.commitIndex
		nextIndex := len(rf.logs)
		rf.mu.Unlock()
		if rf.state != leader {
			break
		}
		if ok := rf.sendAppendEntries(server, &args, &reply); ok{
		}else {
			//fmt.Printf("%v append connect %v fail, nextIndex %v\n", rf.me, server, rf.nextIndex[server])
			break
		}
		//break
		if reply.Success == true{
			fmt.Printf("%v append server %v succeed, nextIndex %v, prenextIndex %v\n", rf.me, server, nextIndex, rf.nextIndex[server])
			rf.nextIndex[server] = nextIndex
			rf.matchIndex[server] = nextIndex-1
			break
		}else{
			//fmt.Printf("server:%v, %v minus 1\n", server, rf.nextIndex[server])
			if(reply.Term > rf.currentTerm){
				return reply
			}
			rf.nextIndex[server]--
		}
		break
	}
	return reply
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if(rf.state != leader){
		isLeader = false
		return index, term, isLeader
	}
	log := Log{command, rf.currentTerm}
	rf.logs = append(rf.logs, log)
	index = rf.nextIndex[rf.me]
	rf.nextIndex[rf.me]++
	rf.matchIndex[rf.me] = len(rf.logs)-1
	term = log.Term
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) transcandidate() {
	rf.mu.Lock()
	rf.state = candidate
	rf.currentTerm += 1
	rf.voteFor = rf.me
	rf.electTime = rand.Intn(200)+200
	rf.waitTime = 0
	rf.mu.Unlock()

	var done sync.WaitGroup
	votecount := 0

	transfollow := false
	for i := 0; i < len(rf.peers) ; i++{
		if i == rf.me{
			continue
		}
		done.Add(1)

		go func(i int){
			//t1 := time.Now().UnixNano()/int64(time.Second)
			reply := rf.SendRequestVote(i)
			if reply.CurrentTerm > rf.currentTerm {
				transfollow = true
				rf.currentTerm = reply.CurrentTerm
				rf.state = follower
				rf.voteFor = -1
				return
			}
			if reply.VoteGrated == true{
				votecount++;
			}
			//t2 := time.Now().UnixNano()/int64(time.Second)
			//fmt.Printf("%v elapsed %v\n", i, t2-t1)
			done.Done()
		}(i)
		if transfollow == true{
			rf.state = follower
			rf.waitTime = 0
			return
		}
	}
	done.Wait()
	fmt.Printf("%v get %v votes.\n", rf.me, votecount)
	if(votecount+1 > rf.serverNum/2){
		rf.state = leader
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = len(rf.logs)
		}
		time.Sleep(50 * time.Millisecond)
		fmt.Printf("%v become a leader\n", rf.me)
	}
	return
}


func (rf *Raft) sendHeartbeatAll(){
	if rf.state != leader {
		return
	}
	var done sync.WaitGroup
	transfollow := false
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me{		//notice don't send heartbeat to yourself
			continue
		}
		if(rf.state != leader){
			return
		}
		done.Add(1)
		go func(i int){
			reply := AppendEntriesReply{}
			if rf.nextIndex[i] >= rf.nextIndex[rf.me] {
				reply = rf.SendHeartbeat(i)
			}else{
				reply = rf.AppendEntries(i)
			}

			if(reply.Term > rf.currentTerm){
				rf.currentTerm = reply.Term
				rf.state = follower
				rf.voteFor = -1
				transfollow = true
			}
			done.Done()
		}(i)
		if transfollow == true {
			return
		}
	}
	done.Wait()
	return
}


func (rf *Raft) followerProcessing(){
	if(rf.waitTime > rf.electTime && rf.voteFor == -1) {
		fmt.Printf("%v Begin an election.\n", rf.me)
		fmt.Printf("%v term is %v\n", rf.me, rf.currentTerm)
		rf.transcandidate()
		return
	}
}


func (rf *Raft) candidateProcessing() {
	if(rf.waitTime > rf.electTime){
		fmt.Printf("%v rebegin an election.\n", rf.me)
		fmt.Printf("%v term is %v\n", rf.me, rf.currentTerm)
		rf.transcandidate()
		return
	}
}

func (rf *Raft) leaderProcessing(){
	rf.waitTime = 0
	go rf.sendHeartbeatAll()    //if a server is lost, it can still transport periodly
	time.Sleep(100 * time.Millisecond)
	rf.increcommitIndex()
	time.Sleep(100 * time.Millisecond)
}

func (rf *Raft) serverWork(){
	for {	if rf.dead == 1 {
			return
	}
		if rf.state == follower{
			rf.followerProcessing()
		}else if rf.state == candidate{
			rf.candidateProcessing()
		}else{
			rf.leaderProcessing()
		}
	}
}


func (rf *Raft) increcommitIndex() {
	for i := rf.commitIndex+1; i < len(rf.logs); i++{
		count := 0
		for j := 0; j < rf.serverNum; j++{
			if(rf.matchIndex[j] >= i){
				count++
			}
		}
		if(count > rf.serverNum/2 && rf.logs[i].Term == rf.currentTerm){
			commitIndex := rf.commitIndex
			rf.commitIndex = i
			for k := commitIndex+1; k <= rf.commitIndex; k++{
				applymsg := ApplyMsg{true, rf.logs[k].Command, k}
				*rf.applyCh <- applymsg
				time.Sleep(10 * time.Millisecond)
				rf.lastApplied = k
			}
			break
		}
	}
}


func (rf *Raft) timer() {
	for{
		time.Sleep(100 * time.Millisecond)
		rf.waitTime += 100
	}
}

func (rf *Raft) init(){
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.serverNum = len(rf.peers)
	rf.electTime = rand.Intn(200)+200
	rf.waitTime = 0
	rf.state = follower
	// log init
	rf.commitIndex = 0
	rf.lastApplied = 0
	log := Log{}
	rf.logs = append(rf.logs, log)
	for i := 0; i < rf.serverNum; i ++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	fmt.Printf("start a client.\n")
	/*
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.serverNum = len(rf.peers)
	rf.electTime = rand.Intn(200)+200
	rf.waitTime = 0
	rf.state = follower
	*/
	rf.init()
	rf.applyCh = &applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.serverWork()
	go rf.timer()
	return rf
}
