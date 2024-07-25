package shardctrler

import (
	"math"
	"sort"
	"sync"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	clientSeq map[int]int // for tracking client requests

	configs []Config // indexed by config num
}

type Op struct {
	Op         string
	Servers    map[int][]string
	GIDs       []int
	Shard      int
	GID        int
	Num        int
	ClientId   int
	RequestSeq int

	// ClientId   int
	// RequestSeq int
}

func (sc *ShardCtrler) sendStart(op Op) bool {
	_, _, isLeader := sc.rf.Start(op)
	return !isLeader
}

func (sc *ShardCtrler) endApply(clientID, requestSeq int) string {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	// Check if the operation was applied successfully.
	if sc.clientSeq[clientID] != requestSeq {
		return ""
	}
	return OK
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{
		Op:         "Join",
		Servers:    args.Servers,
		ClientId:   args.ClientId,
		RequestSeq: args.RequestSeq,
	}
	if reply.WrongLeader = sc.sendStart(op); reply.WrongLeader {
		return
	}
	reply.Err = Err(sc.endApply(args.ClientId, args.RequestSeq))
}

func (sc *ShardCtrler) join(servers map[int][]string) {
	newConfig := sc.newConfig()
	keys := make([]int, 0, len(servers))
	for k := range servers {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	for _, gid := range keys {
		newConfig.Groups[gid] = servers[gid]
	}
	sc.rebalance(&newConfig)
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{
		Op:         "Leave",
		GIDs:       args.GIDs,
		ClientId:   args.ClientId,
		RequestSeq: args.RequestSeq,
	}
	if reply.WrongLeader = sc.sendStart(op); reply.WrongLeader {
		return
	}
	// log.Println("leave")
	reply.Err = Err(sc.endApply(args.ClientId, args.RequestSeq))
}

func (sc *ShardCtrler) leave(GIDs []int) {
	newConfig := sc.newConfig()
	for _, gid := range GIDs {
		delete(newConfig.Groups, gid)
	}
	sc.rebalance(&newConfig)
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{
		Op:         "Move",
		Shard:      args.Shard,
		GID:        args.GID,
		ClientId:   args.ClientId,
		RequestSeq: args.RequestSeq,
	}
	// log.Println("move")
	if reply.WrongLeader = sc.sendStart(op); reply.WrongLeader {
		return
	}
	reply.Err = Err(sc.endApply(args.ClientId, args.RequestSeq))
}

func (sc *ShardCtrler) move(shard, GID int) {
	newConfig := sc.newConfig()
	newConfig.Shards[shard] = GID
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{
		Op:         "Query",
		Num:        args.Num,
		ClientId:   args.ClientId,
		RequestSeq: args.RequestSeq,
	}
	if reply.WrongLeader = sc.sendStart(op); reply.WrongLeader {
		return
	}
	// log.Println("query")
	sc.mu.Lock()
	defer sc.mu.Unlock()
	// Check if the operation was applied successfully.
	if sc.clientSeq[op.ClientId] != op.RequestSeq {
		reply.Err = ""
	} else {
		if args.Num < 0 || args.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
		reply.Err = OK
	}
}

func (sc *ShardCtrler) newConfig() Config {
	oldConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    oldConfig.Num + 1,
		Shards: oldConfig.Shards,
		Groups: make(map[int][]string),
	}

	keys := make([]int, 0, len(oldConfig.Groups))
	for k := range oldConfig.Groups {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	for _, gid := range keys {
		newConfig.Groups[gid] = append([]string{}, oldConfig.Groups[gid]...)
	}

	return newConfig
}

func (sc *ShardCtrler) rebalance(config *Config) {
	if len(config.Groups) == 0 {
		for i := range config.Shards {
			config.Shards[i] = 0
		}
		return
	}

	avgShards := NShards / len(config.Groups)
	shardCount := make(map[int]int)
	keys := make([]int, 0, len(config.Groups))
	for k := range config.Groups {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	for _, gid := range keys {
		shardCount[gid] = 0
	}
	for _, gid := range config.Shards {
		// if gid in groups
		if _, ok := config.Groups[gid]; ok {
			shardCount[gid]++
		}
	}

	for i, gid := range config.Shards {
		if _, ok := config.Groups[gid]; !ok || shardCount[gid] > avgShards {
			minGID := sc.findMin(shardCount)
			if _, ok := config.Groups[gid]; ok {
				shardCount[gid]--
			}
			shardCount[minGID]++
			config.Shards[i] = minGID
		}
	}
}

func (sc *ShardCtrler) findMin(shardCount map[int]int) int {
	minCount := math.MaxInt32
	minGID := 0
	keys := make([]int, 0, len(shardCount))
	for k := range shardCount {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	for _, gid := range keys {
		count := shardCount[gid]
		if count < minCount {
			minCount = count
			minGID = gid
		}
	}
	return minGID
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applyOp(applyMsg raft.ApplyMsg) {
	if applyMsg.CommandValid {
		op := applyMsg.Command.(Op)
		sc.mu.Lock()

		// Check if the operation has already been applied.
		// Apply the operation to the state machine.
		// log.Println(op)
		if seq, ok := sc.clientSeq[op.ClientId]; !ok || seq < op.RequestSeq {
			switch op.Op {
			case "Join":
				sc.join(op.Servers)
			case "Leave":
				sc.leave(op.GIDs)
			case "Move":
				sc.move(op.Shard, op.GID)
			}

			// Update the sequence number for the client.
			sc.clientSeq[op.ClientId] = op.RequestSeq
		}
		sc.mu.Unlock()
		// return applyMsg.CommandIndex
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	sc.clientSeq = make(map[int]int)
	go func() {
		for applyMsg := range sc.applyCh {
			// log.Println("Server", sc.me, "applyOp", applyMsg)
			sc.applyOp(applyMsg)
		}
	}()

	return sc
}
