package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
	id         int
	seq        int
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
	// You'll have to add code here.
	ck.id = int(nrand())
	ck.seq = 0
	return ck
}

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
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.seq++
	args := &GetArgs{Key: key, ClientId: ck.id, RequestSeq: ck.seq}
	reply := &GetReply{}
	ok := ck.servers[ck.lastLeader].Call("KVServer.Get", args, reply)
	if ok && reply.Err == OK {
		return reply.Value
	}

	for {
		for serverNum, server := range ck.servers {
			reply := &GetReply{}
			ok := server.Call("KVServer.Get", args, reply)
			if ok && reply.Err == OK {
				ck.lastLeader = serverNum
				return reply.Value
			}
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seq++
	args := &PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.id, RequestSeq: ck.seq}
	reply := &PutAppendReply{}
	ok := ck.servers[ck.lastLeader].Call("KVServer.PutAppend", args, reply)
	if ok && reply.Err == OK {
		return
	}

	for {
		for serverNum, server := range ck.servers {
			reply := &PutAppendReply{}
			ok := server.Call("KVServer.PutAppend", args, reply)
			if ok && reply.Err == OK {
				ck.lastLeader = serverNum
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
