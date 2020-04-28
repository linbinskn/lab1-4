package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "fmt"

type Clerk struct {
	servers []*labrpc.ClientEnd
	lastleader int
	clerkid int64
	seq int64
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clerkid = nrand()
	ck.seq = 0
	ck.lastleader = 0
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

	// You will have to modify this function.
	ck.seq++
	servernum := len(ck.servers)
	nserver := ck.lastleader
	for {
		fmt.Printf("nserver: %v get once,key:%v, clerkid %v",nserver, key, ck.clerkid)
		args := GetArgs{
			Key : key,
			Clerkid : ck.clerkid,
			Seq : ck.seq,
		}
		reply := GetReply{}
		ok := ck.servers[nserver].Call("KVServer.Get", &args, &reply);
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrloseLeader {
			nserver++
			nserver = nserver % servernum
			continue
		}
		if reply.Err == ErrNoKey {
			break
		}
		if reply.Err == Success {
			ck.lastleader = nserver
			return reply.Value
		}
		nserver = nserver % servernum
	}
	return ""
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
	// You will have to modify this function.
	ck.seq++
	opsucceed := false
	servernum := len(ck.servers)
	nserver := ck.lastleader
	for opsucceed == false {
		fmt.Printf("nserver: %v put once,key:%v,value:%v, clerkid %v.\n",nserver, key, value, ck.clerkid)
		args := PutAppendArgs{
			Key : key,
			Value : value,
			Op : op,
			Clerkid : ck.clerkid,
			Seq : ck.seq,
		}
		reply := PutAppendReply{}
		reply.Err = "nothing"
		ok := ck.servers[nserver].Call("KVServer.PutAppend", &args, &reply);
		fmt.Printf("nserver: %v put once,key:%v,value:%v overrrr. reply %v\n",nserver, key, value, reply.Err)
		if reply.Err == ErrdupTwice {
			break
		}
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrloseLeader || reply.Err == ErrTimeOut{
			nserver++
			nserver = nserver % servernum
			continue
		}
		if reply.Err == Success {
			opsucceed = true
			ck.lastleader = nserver
			break
		}
		nserver = nserver % servernum
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
