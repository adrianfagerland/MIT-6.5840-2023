package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op         string
	Key        string
	Value      string
	Shard      int
	ClientId   int
	RequestSeq int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister
	data      map[string]string
	clientSeq map[int]int
	// below is to store which keys are in the current shard
	mck       *shardctrler.Clerk
	config    shardctrler.Config
	shardKeys map[int]bool
	id        int
	seq       int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Key:        args.Key,
		Op:         "Get",
		ClientId:   args.ClientId,
		RequestSeq: args.RequestSeq,
	}

	if _, _, isLeader := kv.rf.Start(op); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	// if not responsible for the key, return wrong group
	if !kv.shardKeys[key2shard(args.Key)] {
		reply.Err = ErrWrongGroup
		return
	}

	if kv.clientSeq[args.ClientId] == args.RequestSeq {
		value, ok := kv.data[args.Key]
		if ok {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Key:        args.Key,
		Value:      args.Value,
		Op:         args.Op,
		ClientId:   args.ClientId,
		RequestSeq: args.RequestSeq,
	}

	if _, _, isLeader := kv.rf.Start(op); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.shardKeys[key2shard(args.Key)] {
		reply.Err = ErrWrongGroup
		return
	}
	if kv.clientSeq[args.ClientId] == args.RequestSeq {
		reply.Err = OK
	}
}

func (kv *ShardKV) maybeSnapshot(index int) {
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
	e.Encode(kv.shardKeys)
	e.Encode(kv.config)
	data := w.Bytes()
	kv.rf.Snapshot(index, data)
}

func (kv *ShardKV) applySnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var data map[string]string
	var clientSeq map[int]int // TODO update
	var shardKeys map[int]bool
	var config shardctrler.Config
	if d.Decode(&data) != nil ||
		d.Decode(&clientSeq) != nil ||
		d.Decode(&shardKeys) != nil ||
		d.Decode(&config) != nil {
		fmt.Println("Error decoding state")
	} else {
		kv.mu.Lock()
		kv.data = data
		kv.clientSeq = clientSeq
		kv.shardKeys = shardKeys
		kv.config = config
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) applyOp(applyMsg raft.ApplyMsg, snapshot bool) int {
	if applyMsg.CommandValid {
		op := applyMsg.Command.(Op)
		kv.mu.Lock()

		if seq, ok := kv.clientSeq[op.ClientId]; !ok || seq < op.RequestSeq {
			switch op.Op {
			case "Put":
				kv.data[op.Key] = op.Value
				//log.Println(kv.gid, kv.me, "put", op.Key, op.Value)
			case "Append":
				kv.data[op.Key] += op.Value
				//log.Println(kv.gid, kv.me, "append", op.Key, op.Value)
				// case "Config":
				// 	kv.mu.Unlock()
				// 	kv.getConfig()
				// 	kv.mu.Lock()
			}

			kv.clientSeq[op.ClientId] = op.RequestSeq
		}
		kv.mu.Unlock()
		if snapshot {
			kv.maybeSnapshot(applyMsg.CommandIndex)
		}
		return applyMsg.CommandIndex
	}
	kv.applySnapshot(applyMsg.Snapshot)
	if snapshot {
		kv.maybeSnapshot(applyMsg.SnapshotIndex)
	}
	return applyMsg.SnapshotIndex
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.data = make(map[string]string)
	kv.clientSeq = make(map[int]int)
	kv.shardKeys = make(map[int]bool)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.persister = persister
	kv.id = int(nrand())
	kv.seq = 0
	kv.getConfig()

	// TODO persister.SaveStateAndSnapshot()

	kv.applySnapshot(persister.ReadSnapshot())

	snap := true
	if maxraftstate < 0 {
		snap = false
	}
	go func() {
		for applyMsg := range kv.applyCh {
			//// log.Println("Server", kv.me, "applyOp", applyMsg)
			kv.applyOp(applyMsg, snap)
		}
	}()

	go func() {
		for {
			kv.getConfig()
			time.Sleep(5 * time.Millisecond)
		}
	}()

	return kv
}

func (kv *ShardKV) getConfig() {
	// t := time.Now()
	config := kv.mck.Query(kv.config.Num + 1)
	//// log.Println("...", time.Since(t))
	kv.mu.Lock()
	//// log.Println("locking", kv.gid, kv.me)
	if _, leader := kv.rf.GetState(); leader && config.Num > kv.config.Num {
		// lastConfig := kv.mck.Query(config.Num - 1) // TODO maybe delete?
		lastConfig := kv.config
		kv.config = config
		//log.Println(kv.gid, kv.me, "has confignum", config.Num)
		for shard, gid := range config.Shards {
			if gid == kv.gid {
				if !kv.shardKeys[shard] && lastConfig.Shards[shard] != kv.gid && lastConfig.Shards[shard] != 0 {
					//log.Println("confignum is", config.Num)
					//log.Println(kv.gid, kv.me, lastConfig.Shards[shard])
					data, clientSeq := kv.sendGetShard(lastConfig.Groups[lastConfig.Shards[shard]], shard)
					//log.Println(kv.gid, kv.me, "received data", data)
					//log.Println("received clientSeq", clientSeq)
					for key, value := range data {
						if len(kv.data[key]) > len(value) {
							log.Fatal(kv.data[key], ",", value) // TODO probably remove the fatal
						}
						//// log.Println(kv.gid, kv.me, "value received from", lastConfig.Groups[lastConfig.Shards[shard]][0])
						kv.data[key] = value // TODO need to perform put op
						kv.seq++
						kv.rf.Start(Op{
							Op:         "Put",
							Key:        key,
							Value:      value,
							ClientId:   kv.id,
							RequestSeq: kv.seq,
						})
					}
					for client, seq := range clientSeq {
						if seq > kv.clientSeq[client] {
							kv.clientSeq[client] = seq // TODO need to perform an empty op
							kv.rf.Start(Op{
								Op:         "Get",
								Key:        "",
								ClientId:   client,
								RequestSeq: seq,
							})
						}
					}
				}
				kv.shardKeys[shard] = true
			} else {
				kv.shardKeys[shard] = false
			}
		}
		//log.Println(kv.gid, kv.me, "confignum is now", config.Num)
	}
	//// log.Println("unlocking", kv.gid, kv.me)
	kv.mu.Unlock()
}

type GetShardArgs struct {
	Shard      int
	ClientId   int
	RequestSeq int
	ConfigNum  int
}

type GetShardReply struct {
	Err       Err
	Data      map[string]string
	ClientSeq map[int]int
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	op := Op{
		Shard:      args.Shard,
		Op:         "Config",
		ClientId:   args.ClientId,
		RequestSeq: args.RequestSeq,
	}

	if _, _, isLeader := kv.rf.Start(op); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// reject if config is too high
	if args.ConfigNum > kv.config.Num {
		reply.Err = ErrWrongGroup
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	// if not responsible for the key, return wrong group, not for this get
	// if !kv.shardKeys[args.Shard] {
	// 	reply.Err = ErrWrongGroup
	// 	return
	// }

	if kv.clientSeq[args.ClientId] == args.RequestSeq {
		reply.Data = make(map[string]string)
		reply.ClientSeq = make(map[int]int)
		for key, value := range kv.data {
			if key2shard(key) == args.Shard {
				reply.Data[key] = value
			}
		}
		for client, seq := range kv.clientSeq {
			reply.ClientSeq[client] = seq
		}
		reply.Err = OK
		// value, ok := kv.data[args.Key]
		// if ok {
		// 	reply.Value = value
		// } else {
		// 	reply.Err = ErrNoKey
		// }
	}

}

func (kv *ShardKV) sendGetShard(servers []string, shard int) (map[string]string, map[int]int) {
	kv.seq++
	args := GetShardArgs{
		Shard:      shard,
		ClientId:   kv.id,
		RequestSeq: kv.seq,
	}

loop: // TODO deadlocks?
	for {
		for _, server := range servers {
			srv := kv.make_end(server)
			var reply GetShardReply

			// Use a goroutine to send the RPC and receive the reply
			ch := make(chan bool, 1)
			go func() {
				ok := srv.Call("ShardKV.GetShard", &args, &reply)
				if ok {
					ch <- true
				} else {
					ch <- false
				}
			}()

			// Wait for the RPC to complete or for a timeout
			select {
			case ok := <-ch:
				if ok && reply.Err == OK {
					//log.Println("Replying data from", server)
					return reply.Data, reply.ClientSeq
				}
				if reply.Err == ErrNoKey {
					//log.Println("ErrNoKey (1)")
					break loop
				}
			case <-time.After(time.Duration(rand.Intn(2)+1) * time.Second): // adjust timeout as needed
				//log.Println("Timeout")
				kv.mu.Unlock()
				time.Sleep(1 * time.Millisecond)
				kv.mu.Lock()
				continue loop
			}
		}
		time.Sleep(1 * time.Millisecond)
		//log.Println("in deadlock?")
	}
	return nil, nil
}
