package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key        string
	Value      string
	Op         string // "Put" or "Append"
	ClientId   int
	RequestSeq int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister
	data      map[string]string // the key-value store
	clientSeq map[int]int       // for tracking client requests
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		// Fill in the Op struct with the necessary information from args.
		Key:        args.Key,
		Op:         "Get",
		ClientId:   args.ClientId,
		RequestSeq: args.RequestSeq,
	}

	// RPC handler first checks table, only Start()s if seq # > table entry
	// TODO ^^^

	// Start agreement on the operation among the Raft peers.
	// log.Println("Server", kv.me, "Get", args.Key, "Start agreement on the operation among the Raft peers")
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// log.Println("Server", kv.me, "Get", args.Key, "Start agreement on the operation among the Raft peers Done")

	// Wait for the operation to be applied to the state machine.
	// log.Println("Server", kv.me, "Get", args.Key, "Wait for the operation to be applied to the state machine")
	// Create a channel to signal when the operation is applied.
	// for !kv.killed() && atomic.LoadInt32(&kv.lastApplied) < int32(index) {
	// 	time.Sleep(5 * time.Millisecond)
	// }
	// log.Println("Server", kv.me, "Get", args.Key, "Wait for the operation to be applied to the state machine Done")

	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Check if the operation was applied successfully.
	if kv.killed() || kv.clientSeq[op.ClientId] != op.RequestSeq {
		reply.Err = ErrNoKey
	} else {
		value, ok := kv.data[op.Key]
		if !ok {
			reply.Err = OK // TODO ErrNoKey
			reply.Value = ""
		} else {
			reply.Value = value
			reply.Err = OK
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		// Fill in the Op struct with the necessary information from args.
		Key:        args.Key,
		Value:      args.Value,
		Op:         args.Op,
		ClientId:   args.ClientId,
		RequestSeq: args.RequestSeq,
	}

	// Start agreement on the operation among the Raft peers.
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// Wait for the operation to be applied to the state machine.
	// for !kv.killed() && atomic.LoadInt32(&kv.lastApplied) < int32(index) {
	// 	time.Sleep(5 * time.Millisecond)
	// }

	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Check if the operation was applied successfully.
	if kv.killed() || kv.clientSeq[op.ClientId] != op.RequestSeq {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
	}

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) maybeSnapshot(index int) {
	if kv.persister.RaftStateSize() < kv.maxraftstate || kv.maxraftstate == -1 {
		return
	}

	for len(kv.applyCh) > 0 {
		index = kv.applyOp(<-kv.applyCh, false)
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.clientSeq)
	// e.Encode(kv.lastApplied)
	data := w.Bytes()
	// log.Println("Server", kv.me, "doing snapshot", index)
	kv.rf.Snapshot(index, data)
	// log.Println("done", index)
}

func (kv *KVServer) applySnapshot(snapshot []byte) {
	// log.Println(kv.me, "got snapshot that i need to apply")

	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	//log.Println("Server", rf.me, "reading persist")
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var data map[string]string
	var clientSeq map[int]int
	// var lastApplied int32
	if d.Decode(&data) != nil ||
		d.Decode(&clientSeq) != nil {
		// d.Decode(&lastApplied) != nil ||
		fmt.Println("Error decoding state")
	} else {
		kv.mu.Lock()
		kv.data = data
		// log.Println(kv.data)
		kv.clientSeq = clientSeq
		// log.Println(kv.clientSeq)
		// kv.lastApplied = lastApplied
		// log.Println(kv.lastApplied)
		kv.mu.Unlock()
	}

	// log.Println(kv.me, "applied snapshot")

	// TODO Update the sequence number for the client.
	// kv.clientSeq[op.ClientId] = op.RequestSeq
	// atomic.StoreInt32(&kv.lastApplied, int32(snap.SnapshotIndex)) //TODO DElete?
}

func (kv *KVServer) applyOp(applyMsg raft.ApplyMsg, snapshot bool) int {
	if applyMsg.CommandValid {
		op := applyMsg.Command.(Op)
		kv.mu.Lock()

		// Check if the operation has already been applied.
		if seq, ok := kv.clientSeq[op.ClientId]; !ok || seq < op.RequestSeq {
			// Apply the operation to the state machine.
			switch op.Op {
			case "Put":
				kv.data[op.Key] = op.Value
			case "Append":
				kv.data[op.Key] += op.Value
			}

			// Update the sequence number for the client.
			kv.clientSeq[op.ClientId] = op.RequestSeq
		}
		kv.mu.Unlock()
		if snapshot {
			kv.maybeSnapshot(applyMsg.CommandIndex)
			// log.Println("server,", kv.me, kv.clientSeq)
		}
		return applyMsg.CommandIndex

		// Update the index of the last applied command.
		// atomic.StoreInt32(&kv.lastApplied, int32(applyMsg.CommandIndex))

	} else {
		// TODO
		kv.applySnapshot(applyMsg.Snapshot)
		if snapshot {
			kv.maybeSnapshot(applyMsg.SnapshotIndex)
		}
		return applyMsg.SnapshotIndex
	}
}

// log.Println("maybe snap", applyMsg.CommandIndex)
// log.Println("done maybe snap", applyMsg.CommandIndex)

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	// log.Println("Server", me, "StartKVServer")
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.clientSeq = make(map[int]int)

	kv.applyCh = make(chan raft.ApplyMsg, 1500)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister

	kv.applySnapshot(persister.ReadSnapshot())
	// Start a goroutine to apply operations to the state machine.
	snap := true
	if maxraftstate < 0 {
		snap = false
	}
	go func() {
		for applyMsg := range kv.applyCh {
			// log.Println("Server", kv.me, "applyOp", applyMsg)
			kv.applyOp(applyMsg, snap)
		}
	}()

	// You may need initialization code here.

	return kv
}
