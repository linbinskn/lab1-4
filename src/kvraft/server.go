package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
	//"fmt"
)

const Debug = 0
const TimeOut = time.Duration(3 * time.Second)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct{
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string
	Clerkid   int64
	Seq       int64
}

type Matchlog struct{
	index int
	term int
	op Op
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	statemachine map[string]string
	remindchan   map[int](chan Err)
	matchlogs    map[int]Matchlog
	duptable     map[int64]int64
	deadchan chan bool
}

func (kv *KVServer) MatchlogAdd(index int, term int, op Op) {
	matchlog := Matchlog{}
	matchlog.index = index
	matchlog.term = term
	matchlog.op = op
	kv.matchlogs[index] = matchlog
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{}
	op.Operation = "Get"
	op.Key = args.Key
	op.Clerkid = args.Clerkid
	op.Seq = args.Seq
	if _, isleader := kv.rf.GetState(); !isleader{
		reply.Err = ErrWrongLeader
		return
	}
	index, term, _ := kv.rf.Start(op)
	kv.mu.Lock()
	/*
	if value, ok := kv.duptable[args.Clerkid]; ok && args.Seq <= value{
		reply.Err = ErrdupTwice
		kv.mu.Unlock()
		return
	}
	*/
	//index, term, isLeader := kv.rf.Start(op)
	//if !isLeader {
	//	reply.Err = ErrWrongLeader
	//	kv.mu.Unlock()
	//	return
	//}
	//fmt.Printf("Get server %v is leader key:%v, clerkid:%v, index:%v\n", kv.me, args.Key, args.Clerkid, index)
	kv.MatchlogAdd(index, term, op)
	kv.remindchan[index] = make(chan Err)
	kv.mu.Unlock()
	select {
	case <- time.After(TimeOut):
		kv.mu.Lock()
		delete(kv.remindchan, index)
		reply.Err = ErrTimeOut
		//fmt.Printf("leader %v timeout\n", kv.me)
		kv.mu.Unlock()
		return
	case reply.Err =<-kv.remindchan[index]:
		if reply.Err == ErrloseLeader {
			return
		}
		kv.mu.Lock()
		if value, ok := kv.statemachine[op.Key]; ok{
			reply.Value = value
			reply.Err = Success
		}else {
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
		return

	}
	//reply.Err =<-kv.remindchan[index]
	kv.mu.Lock()
	if reply.Err == ErrloseLeader {
		return
	}
	if value, ok := kv.statemachine[op.Key]; ok{
		reply.Value = value
		reply.Err = Success
	}else {
		reply.Err = ErrNoKey
	}
	kv.mu.Unlock()
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//fmt.Printf("Get into PutAppend server %v\n", kv.me)
	op := Op{}
	op.Operation = args.Op
	op.Key = args.Key
	op.Value = args.Value
	op.Clerkid = args.Clerkid
	op.Seq = args.Seq
	kv.mu.Lock()
	if value, ok := kv.duptable[args.Clerkid]; ok && args.Seq <= value{
		reply.Err = ErrdupTwice
		kv.mu.Unlock()
		return
	}
	if _, isleader := kv.rf.GetState(); !isleader{
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	index, term, _ := kv.rf.Start(op)
	//index, term, isLeader := kv.rf.Start(op)
	//if !isLeader {
	//	reply.Err = ErrWrongLeader
	//	kv.mu.Unlock()
	//	return
	//}
	//fmt.Printf("Put server %v is leader, key:%v, value:%v, clerkid:%v, index:%v\n", kv.me, op.Key, op.Value, op.Clerkid, index)
	kv.MatchlogAdd(index, term, op)
	kv.remindchan[index] = make(chan Err)
	kv.mu.Unlock()
	select {
	case <- time.After(TimeOut):
		kv.mu.Lock()
		delete(kv.remindchan, index)
		reply.Err = ErrTimeOut
		kv.mu.Unlock()
		return
	case reply.Err =<-kv.remindchan[index]:
		//reply.Err=<-kv.remindchan[index]
		if reply.Err == ErrloseLeader {
			return
		}
		reply.Err = Success
		return

	}
	reply.Err=<-kv.remindchan[index]
	//fmt.Printf("server %v get remindchan reply.Err %v\n", kv.me,reply.Err)
	if reply.Err == ErrloseLeader {
		return
	}
	reply.Err = Success
	return
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
	close(kv.deadchan)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func max(a int64, b int64) int64 {
	if a > b{
		return a
	}
	return b
}

func (kv *KVServer) apply() {
	for {
		select{
		case applymsg :=<-kv.applyCh:
			index := applymsg.CommandIndex
			op := applymsg.Command.(Op)
			term := applymsg.Term
			leaderid := applymsg.LeaderId
			//fmt.Printf("server %v msg come key:%v, value:%v, seq:%v, clerkid:%v, leaderid:%v\n",kv.me,op.Key, op.Value, op.Seq, op.Clerkid, leaderid)
			kv.mu.Lock()
			if value, ok := kv.duptable[op.Clerkid]; ok && op.Seq <= value && op.Operation != "Get"{
				//fmt.Printf("server %v seq duplicate, seq:%v maxseq:%v\n", kv.me, op.Seq, value)
				kv.mu.Unlock()
				continue
			}

			if op.Operation == "Put" {
				kv.statemachine[op.Key] = op.Value
			}else if op.Operation == "Append" {
				if _, ok := kv.statemachine[op.Key]; ok {
					kv.statemachine[op.Key] += op.Value
				}else {
					kv.statemachine[op.Key] = op.Value
				}
			}
			kv.duptable[op.Clerkid] = max(op.Seq, kv.duptable[op.Clerkid])
			/*
			if _, isleader := kv.rf.GetState(); !isleader{
				continue
			}
			if leaderid != kv.me {
				continue
			}
			*/
			if leaderid != kv.me {
				kv.mu.Unlock()
				continue
			}

			if _, ok := kv.remindchan[index]; !ok {
				//fmt.Printf("server %v index %v chan has been deleted\n", kv.me, index)
				kv.mu.Unlock()
				continue
			}
			var err Err
			if op != kv.matchlogs[index].op || term != kv.matchlogs[index].term {
				err = ErrloseLeader
			}else {
				err = Success
			}
			kv.mu.Unlock()
			kv.remindchan[index] <- err
			//fmt.Printf("server %v err insert into remindchan by channel\n", kv.me)
			time.Sleep(5 * time.Millisecond)
		case <-kv.deadchan:
			//fmt.Printf("close the chan, server %v\n", kv.me)
			return
		}
		}
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

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.deadchan = make(chan bool)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.statemachine = make(map[string]string)
	kv.remindchan = make(map[int](chan Err))
	kv.matchlogs = make(map[int]Matchlog)
	kv.duptable = make(map[int64]int64)

	go kv.apply()
	// You may need initialization code here.

	return kv
}
