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
//import "fmt"
import "log"
import "bytes"
import "../labgob"


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
	Term int
	LeaderId int
}

type Log struct {
	Command	     interface{}
	Term         int
	LeaderId     int
}
//
// A Go object implementing a single Raft peer.
//

var applylock sync.Mutex
var heartbeatlock sync.Mutex
var requestvotelock sync.Mutex
var appendentrieslock sync.Mutex
var electlock sync.Mutex

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int
	voteFor     int
	voteCand    bool
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

func max(a, b int) int {
	if(a < b){
		return b
	}
	return a
}
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.commitIndex)
	//e.Encode(rf.lastApplied)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var commitIndex int
	var lastApplied int
	logs := []Log{}
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&voteFor) != nil ||
	   d.Decode(&commitIndex) != nil ||
	   //d.Decode(&lastApplied) != nil ||
	   d.Decode(&logs) != nil {
		log.Fatalf("persister decode fail\n")
	   } else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.logs = logs
		rf.commitIndex = commitIndex
		rf.lastApplied = lastApplied
		rf.mu.Unlock()
	   }
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
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
	ConnectFail   bool
	ConflictIndex int
	ConflictTerm  int
	Hashad	      bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("%v requestvote term %v state %v from %v argsterm %v\n", rf.me, rf.currentTerm, rf.state, args.CandidateId, args.Term)
	if args.Term < rf.currentTerm {
		reply.VoteGrated = false
		reply.CurrentTerm = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm{
		rf.state = follower
		rf.voteFor = -1
		rf.currentTerm = args.Term
	}
	voteflag := (rf.voteFor == -1 || rf.voteFor == args.CandidateId)
	uptodate := false
	if args.LastlogTerm > rf.logs[len(rf.logs)-1].Term{
		uptodate = true
	}
	if args.LastlogTerm == rf.logs[len(rf.logs)-1].Term && args.LastlogIndex >= len(rf.logs)-1 {
		uptodate = true
	}
	if voteflag && uptodate {	//Only Term is enough
		rf.waitTime = 0
		rf.currentTerm = args.Term        //  don't be hurry
		rf.voteFor = args.CandidateId
		reply.CurrentTerm = rf.currentTerm
		reply.VoteGrated = true
		rf.voteCand = true
		//fmt.Printf("%v vote the %v\n", rf.me, args.CandidateId)
	}else if(rf.currentTerm < args.Term){
		rf.currentTerm = args.Term
		rf.state = follower
	}
	go rf.persist()
	return
}


func (rf *Raft) ReceiveAppendEntries(args *AppendEntries, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(args.Entries)>0{
	//fmt.Printf("%v receive appendentries from %v Pervlogindex %v\n", rf.me, args.LeaderId, args.Pervlogindex)
	}
	var match bool
	if args.Term > rf.currentTerm {
		rf.state = follower
		rf.voteFor = -1
		rf.waitTime = 0
		rf.currentTerm = args.Term
	}
	if args.Pervlogindex >= len(rf.logs) {
		match = false
	}else {
		match = (rf.logs[args.Pervlogindex].Term == args.Pervlogterm)
	}
	if args.Term < rf.currentTerm {
		//fmt.Printf("me:%v, args.Term:%v, currentTerm:%v, match:%v\n", rf.me, args.Term, rf.currentTerm, match)
		reply.Term = rf.currentTerm
		reply.Success = false
		go rf.persist()
		return
	}
	rf.state = follower
	rf.voteCand = false
	rf.waitTime = 0
	if match == false {
		if args.Pervlogindex > len(rf.logs)-1 {
			reply.ConflictIndex = len(rf.logs)
			reply.ConflictTerm = -1
		}else {
			term := rf.logs[args.Pervlogindex].Term
			for i, log := range rf.logs {
				if log.Term == term {
					reply.ConflictIndex = i
					break
				}
			}
			reply.ConflictTerm = term
		}
		reply.Term = rf.currentTerm
		reply.Success = false
		go rf.persist()
		return
	}
	if len(args.Entries) == 0 {
		reply.Term = rf.currentTerm
		reply.Success = true
		reply.Hashad = true
		if rf.commitIndex < args.LeaderCommit{
			rf.commitIndex = min(args.LeaderCommit, args.Pervlogindex+len(args.Entries))
		}
		go rf.persist()
		return
	}

	entrylastindex := args.Pervlogindex+len(args.Entries)
	var i int
	for i = args.Pervlogindex+1; i < len(rf.logs) && i <= entrylastindex; i++{
		if rf.logs[i].Term != args.Entries[i-args.Pervlogindex-1].Term {
			rf.logs = rf.logs[:i]
			break
		}
	}
	if i > entrylastindex {
		reply.Hashad = true
	}else {
		rf.logs = append(rf.logs, args.Entries[i-args.Pervlogindex-1:]...)
	}
	if rf.commitIndex < args.LeaderCommit{
		rf.commitIndex = min(args.LeaderCommit, args.Pervlogindex+len(args.Entries))
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	go rf.persist()
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
	count := 0
	for {
		ok := rf.peers[server].Call("Raft.ReceiveAppendEntries", args, reply)
		if ok || count > 5 {
			return ok
		}
		count ++
	}
}

func (rf *Raft) SendRequestVote(serverid int, args RequestVoteArgs) RequestVoteReply{
	reply := RequestVoteReply{}
	reply.VoteGrated = false

	if ok := rf.sendRequestVote(serverid, &args, &reply);ok{
		return reply
	}

	return reply
}



func (rf *Raft) AppendEntries(server int, term int) AppendEntriesReply{
	reply := AppendEntriesReply{}
	args := AppendEntries{}
	reply.Success = false
	reply.ConnectFail = false
	reply.Hashad = false
	rf.mu.Lock()
	if rf.state != leader || term != rf.currentTerm{
		rf.mu.Unlock()
		return reply
	}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	if rf.nextIndex[server] < len(rf.logs){
		args.Entries = rf.logs[rf.nextIndex[server]:]
	}
	args.Pervlogindex = rf.nextIndex[server]-1
	args.Pervlogterm = rf.logs[args.Pervlogindex].Term
	args.LeaderCommit = rf.commitIndex
	nextIndex := args.Pervlogindex+len(args.Entries)+1
	rf.mu.Unlock()
	if rf.state != leader {
		return reply
	}
	if ok := rf.sendAppendEntries(server, &args, &reply); ok{
	}else {
		reply.ConnectFail = true
		//fmt.Printf("%v append connect %v fail, nextIndex %v\n", rf.me, server, rf.nextIndex[server])
		return reply
	}
	if args.Term != rf.currentTerm {
		return reply
	}
	if reply.Hashad == true{
		return reply
	}
	if reply.Success == true{
		//fmt.Printf("%v append server %v succeed, nextIndex %v, prenextIndex %v\n", rf.me, server, max(nextIndex, rf.nextIndex[server]), rf.nextIndex[server])
		rf.mu.Lock()
		rf.nextIndex[server] = max(nextIndex, rf.nextIndex[server])
		rf.matchIndex[server] = max(nextIndex-1, rf.matchIndex[server])
		rf.mu.Unlock()
		return reply
	}else{
		//fmt.Printf("server:%v, %v minus 1\n", server, rf.nextIndex[server])
		if(reply.Term > rf.currentTerm){
			return reply
		}
		nextindex := -1
		if rf.logs[args.Pervlogindex+len(args.Entries)].Term <= reply.ConflictTerm || reply.ConflictTerm == -1{
			rf.nextIndex[server] = reply.ConflictIndex
			return reply
		}
		for i := len(rf.logs)-2 ; i >= 0; i -- {
			if rf.logs[i].Term == reply.ConflictTerm {
				rf.nextIndex[server] = i+1
				nextindex = i+1
				return reply
			}
		}
		if nextindex == -1 {
			rf.nextIndex[server] = reply.ConflictIndex
		}
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

	if(rf.state != leader){
		isLeader = false
		return index, term, isLeader
	}
	//fmt.Printf("leader %v receive log, index %v\n",rf.me,len(rf.logs)+1)
	log := Log{command, rf.currentTerm, rf.me}
	rf.mu.Lock()
	rf.logs = append(rf.logs, log)
	index = rf.nextIndex[rf.me]
	rf.nextIndex[rf.me] = len(rf.logs)
	rf.matchIndex[rf.me] = len(rf.logs)-1
	rf.mu.Unlock()
	term = log.Term
	go rf.persist()
	go rf.sendHeartbeatAll(rf.currentTerm)
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) transcandidate(args RequestVoteArgs) {
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
			reply := rf.SendRequestVote(i, args)
			if args.Term != rf.currentTerm{
				return
			}
			if reply.CurrentTerm > rf.currentTerm {
				transfollow = true
				rf.mu.Lock()
				rf.currentTerm = reply.CurrentTerm
				rf.state = follower
				rf.voteFor = -1
				rf.mu.Unlock()
				return
			}
			if reply.VoteGrated == true{
				rf.mu.Lock();
				votecount++;
				rf.mu.Unlock();
				if(votecount+1 > rf.serverNum/2 && rf.state == candidate){
					rf.mu.Lock()
					if rf.state == leader {
						rf.mu.Unlock()
						return
					}
					rf.state = leader
					for i := 0; i < len(rf.nextIndex); i++ {
						rf.nextIndex[i] = len(rf.logs)
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
					go rf.persist()
					//fmt.Printf("%v become a leader\n", rf.me)
					go rf.sendHeartbeatAll(rf.currentTerm)
				}
			}
			//t2 := time.Now().UnixNano()/int64(time.Second)
			//fmt.Printf("%v elapsed %v\n", i, t2-t1)
			done.Done()
		}(i)
		if transfollow == true{
			rf.state = follower
			return
		}
	}
	done.Wait()
	//fmt.Printf("%v get %v votes.\n", rf.me, votecount)
	//if(votecount+1 > rf.serverNum/2){
	//	rf.state = leader
	//	for i := 0; i < len(rf.nextIndex); i++ {
	//		rf.nextIndex[i] = len(rf.logs)
	//		//rf.matchIndex[i] = 0
	//	}
	//	fmt.Printf("%v become a leader\n", rf.me)
	//}
	//rf.persist()
	return
}


func (rf *Raft) sendHeartbeatAll(interm int){
	if rf.state != leader || interm != rf.currentTerm{
		return
	}
	var done sync.WaitGroup
	transfollow := false
	term := rf.currentTerm
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me{		//notice don't send heartbeat to yourself
			continue
		}
		if(rf.state != leader){
			return
		}
		failconnect := 0
		done.Add(1)
		go func(i int){
			reply := AppendEntriesReply{}
			reply = rf.AppendEntries(i, term)
			if term != rf.currentTerm {
				done.Done()
				return
			}
			if reply.ConnectFail == true {
				failconnect ++
			}

			if(reply.Term > rf.currentTerm){
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				rf.state = follower
				rf.voteFor = -1
				rf.mu.Unlock()
				transfollow = true
			}
			done.Done()
		}(i)
		if transfollow == true {
			return
		}
	}
	done.Wait()
	go rf.persist()
	return
}


func (rf *Raft) followerProcessing(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if(rf.waitTime > rf.electTime && rf.voteCand == false) {
		rf.waitTime = 0
		rf.state = candidate
		rf.currentTerm += 1
		rf.voteFor = rf.me
		rf.electTime = rand.Intn(150)+200
		rf.waitTime = 0

		args := RequestVoteArgs{}
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		args.LastlogIndex = len(rf.logs)-1
		args.LastlogTerm = rf.logs[len(rf.logs)-1].Term
		//fmt.Printf("%v Begin an election.\n", rf.me)
		//fmt.Printf("%v term is %v\n", rf.me, rf.currentTerm)
		go rf.transcandidate(args)
		return
	}
}


func (rf *Raft) candidateProcessing() {
	if(rf.waitTime > rf.electTime){
		rf.mu.Lock()
		rf.waitTime = 0
		rf.state = candidate
		rf.currentTerm += 1
		rf.voteFor = rf.me
		rf.electTime = rand.Intn(150)+200
		rf.waitTime = 0

		args := RequestVoteArgs{}
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		args.LastlogIndex = len(rf.logs)-1
		args.LastlogTerm = rf.logs[len(rf.logs)-1].Term
		rf.mu.Unlock()
		//fmt.Printf("%v rebegin an election.\n", rf.me)
		//fmt.Printf("%v term is %v\n", rf.me, rf.currentTerm)
		go rf.transcandidate(args)
		return
	}
}

func (rf *Raft) leaderProcessing(){
	for{
		if rf.state == leader {
			go rf.sendHeartbeatAll(rf.currentTerm)
			time.Sleep(100 * time.Millisecond)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (rf *Raft) statelog() {
	for {
		time.Sleep(500 * time.Millisecond)
		//fmt.Printf("%v is now state %v, term is %v\n", rf.me, rf.state, rf.currentTerm)
	}
}

func (rf *Raft) serverWork(){
	for {	if rf.dead == 1 {
			return
	}
		time.Sleep(5 * time.Millisecond)
		if rf.state == follower{
			rf.followerProcessing()
		}else if rf.state == candidate{
			rf.candidateProcessing()
		}else{
			rf.waitTime = 0
		}
	}
}

func (rf *Raft) Timer(){
	for {
		time.Sleep(5 * time.Millisecond)
		rf.mu.Lock()
		rf.waitTime += 5
		rf.mu.Unlock()
	}
}


func (rf *Raft) increcommitIndex() {
	for{
	if rf.state != leader {
		time.Sleep(50 * time.Millisecond)
		continue
	}
	for i := rf.commitIndex+1; i < len(rf.logs); i++{
		count := 0
		for j := 0; j < rf.serverNum; j++{
			if(rf.matchIndex[j] >= i){
				count++
			}
		}
		if(count > rf.serverNum/2 && rf.logs[i].Term == rf.currentTerm && rf.state == leader){
			rf.commitIndex = i
			//fmt.Printf("leader %v commitindex succeed update to %v\n", rf.me, rf.commitIndex)
			break
		}
	}
	time.Sleep(10 * time.Millisecond)
}
}

func (rf *Raft) applyentries(){
	for {
	rf.mu.Lock()
	if rf.lastApplied < rf.commitIndex {
		lastApplied := rf.lastApplied
		commitIndex := rf.commitIndex
		rf.mu.Unlock()
		for k := lastApplied+1; k <= commitIndex; k++{
			applymsg := ApplyMsg{true, rf.logs[k].Command, k, rf.logs[k].Term, rf.logs[k].LeaderId}
			//fmt.Printf("%v %v lastApplied update to  %v\n", rf.state, rf.me, k)
			*rf.applyCh <- applymsg
			rf.lastApplied = k
			//fmt.Printf("%v %v lastApplied succeed update to  %v\n", rf.state, rf.me, rf.lastApplied)
		}
	}else {
		rf.mu.Unlock()
	}
	time.Sleep(10 * time.Millisecond)
	}
}


func (rf *Raft) init(){
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.voteCand = false
	rf.serverNum = len(rf.peers)
	rf.electTime = rand.Intn(150)+200
	rf.waitTime = 0
	rf.state = follower
	// log init
	rf.commitIndex = 0
	rf.lastApplied = 0
	log := Log{}
	log.Term = 0
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
	//fmt.Printf("start a client.\n")
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
	go rf.Timer()
	go rf.increcommitIndex()
	//go rf.statelog()
	go rf.applyentries()
	go rf.leaderProcessing()
	return rf
}
