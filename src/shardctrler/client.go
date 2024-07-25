package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	ck.id = int(nrand())
	ck.seq = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	ck.seq++
	args.Num = num
	args.ClientId = ck.id
	args.RequestSeq = ck.seq
	var reply QueryReply
	ok := ck.servers[ck.lastLeader].Call("ShardCtrler.Query", args, &reply)
	if ok && !reply.WrongLeader && reply.Err == OK {
		return reply.Config
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && !reply.WrongLeader && reply.Err == OK {
				return reply.Config
			}
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	ck.seq++
	args.Servers = servers
	args.ClientId = ck.id
	args.RequestSeq = ck.seq
	var reply JoinReply
	ok := ck.servers[ck.lastLeader].Call("ShardCtrler.Join", args, &reply)
	if ok && !reply.WrongLeader && reply.Err == OK {
		return
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && !reply.WrongLeader && reply.Err == OK {
				return
			}
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	ck.seq++
	args.GIDs = gids
	args.ClientId = ck.id
	args.RequestSeq = ck.seq
	var reply LeaveReply
	ok := ck.servers[ck.lastLeader].Call("ShardCtrler.Leave", args, &reply)
	if ok && !reply.WrongLeader && reply.Err == OK {
		return
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && !reply.WrongLeader && reply.Err == OK {
				return
			}
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	ck.seq++
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.id
	args.RequestSeq = ck.seq
	var reply MoveReply
	ok := ck.servers[ck.lastLeader].Call("ShardCtrler.Move", args, &reply)
	if ok && !reply.WrongLeader && reply.Err == OK {
		return
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && !reply.WrongLeader && reply.Err == OK {
				return
			}
		}
		time.Sleep(1 * time.Millisecond)
	}
}
