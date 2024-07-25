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
	"fmt"

	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

const HeartbeatInterval = 350 * time.Millisecond
const ElectionTimeout = 1000 * time.Millisecond

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int
	votedFor      int
	isLeader      bool
	lastHeartbeat time.Time

	log         [][2]interface{}
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyCh    chan ApplyMsg
	applyMsgCh chan ApplyMsg

	lastIncludedIndex int // TODO need to store these two in persist?
	lastIncludedTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.isLeader
}

type Rejection struct {
	XTerm  int
	XIndex int
	XLen   int
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	CurrentTerm := rf.currentTerm
	VotedFor := rf.votedFor
	Log := rf.log
	LastIncludedIndex := rf.lastIncludedIndex
	LastIncludedTerm := rf.lastIncludedTerm
	e.Encode(CurrentTerm)
	e.Encode(VotedFor)
	e.Encode(Log)
	e.Encode(LastIncludedIndex)
	e.Encode(LastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	//log.Println("Server", rf.me, "reading persist")
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var Log [][2]interface{}
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&Log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		fmt.Println("Error decoding state")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = Log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
		rf.mu.Unlock()
	}
	//log.Println("Server", rf.me, "read persist")
	if len(rf.persister.ReadSnapshot()) != 0 {
		rf.Snapshot(lastIncludedIndex, rf.persister.ReadSnapshot())
	}

}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// trim the log
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// if index-rf.lastIncludedIndex <= 0 || index-rf.lastIncludedIndex > len(rf.log) {
	//// //log.Println("Server ", rf.me, "snapshot is returning because length of log is", len(rf.log), "and index is", index)
	//// //log.Println("index-rf.lastIncludedIndex is ", index-rf.lastIncludedIndex)
	// 	return
	// }
	//log.Println("Server ", rf.me, " is trimming log from ", len(rf.log), " to ", index-rf.lastIncludedIndex)
	// change the necessary states in raft
	// rf.commitIndex -= index
	// rf.lastApplied -= index
	// for i := range rf.nextIndex {
	// 	rf.nextIndex[i] -= index - rf.lastIncludedIndex - 1
	// 	rf.matchIndex[i] -= index - rf.lastIncludedIndex - 1
	// }
	//log.Println("Server ", rf.me, " commitindex is ", rf.commitIndex)
	//log.Println("Server ", rf.me, " lastApplied is ", rf.lastApplied)
	//log.Println("Server ", rf.me, " nextIndex is ", rf.nextIndex)
	//log.Println("Server ", rf.me, " matchIndex is ", rf.matchIndex)
	if len(rf.log) > 0 {
		rf.lastIncludedTerm = rf.log[len(rf.log)-1][1].(int)
	}
	if index-rf.lastIncludedIndex < 0 {
		//log.Println("sserver", rf.me)
		// print the contents of rf.applymshch
		validLast := false
		var last ApplyMsg
		for len(rf.applyMsgCh) > 0 {
			msg := <-rf.applyMsgCh
			if !msg.CommandValid {
				last = msg
				validLast = true
			}
		}
		if validLast && !last.CommandValid {
			//log.Println("what", rf.me)
			applymsg := ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      last.Snapshot,
				SnapshotTerm:  last.SnapshotTerm,
				SnapshotIndex: last.SnapshotIndex,
			}
			rf.applyMsgCh <- applymsg
		} else {
			// fmt.Println(rf.me, "returning, check this")
		}
		return
	}
	// for len(rf.applyMsgCh) > 0 {
	// 	<-rf.applyMsgCh
	// }
	// select {
	// case msg := <-rf.applyCh:
	// 	//log.Println("caught", msg)
	// default:
	// 	// This case is selected when no other case is ready.
	// }
	if index-rf.lastIncludedIndex <= len(rf.log) {
		//log.Println("Server ", rf.me, "removing logs", rf.log[:index-rf.lastIncludedIndex])
		rf.log = rf.log[index-rf.lastIncludedIndex:]
		//log.Println("Server ", rf.me, "log is now", rf.log)
	}
	// validLast := false
	// var last ApplyMsg
	// for len(rf.applyMsgCh) > 0 {
	// 	msg := <-rf.applyMsgCh
	// 	if !msg.CommandValid {
	// 		last = msg
	// 		validLast = true
	// 	}
	// }
	// if validLast && !last.CommandValid && last.SnapshotIndex == index {
	// 	// //log.Println("what", rf.me)
	// 	applymsg := ApplyMsg{
	// 		CommandValid:  false,
	// 		SnapshotValid: true,
	// 		Snapshot:      last.Snapshot,
	// 		SnapshotTerm:  last.SnapshotTerm,
	// 		SnapshotIndex: last.SnapshotIndex,
	// 	}
	// 	rf.applyMsgCh <- applymsg
	// }
	// // empty rf.applych if there is something on it
	// // note that we want to empty rf.applych, not rf.applymsgch
	// //log.Println(len(rf.applyCh))

	rf.lastIncludedIndex = index
	//log.Println("Server ", rf.me, "lastincludedindex is", rf.lastIncludedIndex)
	//log.Println("Server ", rf.me, "lastincludedterm is", rf.lastIncludedTerm)
	//log.Println("Server ", rf.me, " changing commitindex from ", rf.commitIndex, " to ", int(math.Max(float64(rf.commitIndex), float64(rf.lastIncludedIndex))))
	rf.commitIndex = int(math.Max(float64(rf.commitIndex), float64(rf.lastIncludedIndex)))
	// TODO maybe update lastapplied?
	//log.Println("Server ", rf.me, "changing rf.lastapplied from ", rf.lastApplied, "to ", int(math.Max(float64(rf.lastIncludedIndex), float64(rf.lastApplied))))
	// rf.lastApplied = rf.lastIncludedIndex
	rf.lastApplied = int(math.Max(float64(rf.lastIncludedIndex), float64(rf.lastApplied)))

	// persist the snapshot
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.Save(data, snapshot)
	if rf.isLeader && snapshot != nil {
		for i := range rf.peers {
			if i != rf.me && (rf.nextIndex[i]-1-rf.lastIncludedIndex < 0 || index == rf.lastIncludedIndex) {
				// if i != rf.me {
				rf.nextIndex[i] = rf.lastIncludedIndex + 1
				go rf.sendInstallSnapshot(i)
			}
		}
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	//log.Println(rf.me, "got IS from", args.LeaderId)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		//log.Println("Server ", rf.me, "snapshot is returning because term is", args.Term, "and current term is", rf.currentTerm)
		rf.mu.Unlock()
		return
	}
	rf.lastHeartbeat = time.Now()
	if args.LastIncludedIndex < rf.lastIncludedIndex {
		rf.mu.Unlock()
		//log.Println("new")
		// time out the call
		time.Sleep(time.Second)
		return
	}
	applymsg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	// //log.Println("Server ", rf.me, " is sending snapshot to applymsg channel")
	for len(rf.applyMsgCh) > 0 {
		<-rf.applyMsgCh
	}
	rf.applyMsgCh <- applymsg
	rf.lastIncludedTerm = args.LastIncludedTerm
	//log.Println("Server ", rf.me, "removing all items from log")
	// log change in lastapplied
	//log.Println(rf.me, "lastapplied from", rf.lastApplied, "to", args.LastIncludedIndex)
	rf.lastApplied = args.LastIncludedIndex
	rf.log = nil
	rf.mu.Unlock()
	rf.Snapshot(args.LastIncludedIndex, args.Data)
}

// send InstallSnapshot RPC
func (rf *Raft) sendInstallSnapshot(server int) {
	//log.Println(rf.me, "sending snap", server)
	rf.mu.Lock()
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := &InstallSnapshotReply{}
	if !rf.isLeader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if (reply.Term > args.Term) || (reply.Term > rf.currentTerm) {
			//log.Println("removing myself as leader because got a reply for installsnapshot from server ", server, " with term ", reply.Term)
			//log.Println("But checking if I can commit something first")
			rf.checkCommitIndex()
			rf.currentTerm = reply.Term
			rf.isLeader = false
			rf.votedFor = -1
			rf.persist()
			return
		}
		//// //log.Println("Server ", rf.me, " updating nextindex from ", rf.nextIndex[server], " to ", rf.lastIncludedIndex+1, " for server ", server)
		rf.nextIndex[server] = rf.lastIncludedIndex + 1
	} else {
		//log.Println("call from server ", args.LeaderId, " to server ", server, " failed with snapshot ", args.Data)
		// go rf.sendInstallSnapshot(server)
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//log.Println("Server ", rf.me, "received request for vote from ", args.CandidateId, " for term: ", args.Term)
	//log.Println("Server ", rf.me, " own term is ", rf.currentTerm)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		//log.Println("Server ", rf.me, "received request for vote from ", args.CandidateId, " but args.Term < rf.currentTerm")
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.isLeader {
			//log.Println("Server ", rf.me, " is removing itself as leader because of term ", args.Term)
		}
		rf.isLeader = false
		//log.Println("Server ", rf.me, " votedfor is ", rf.votedFor, " for term ", args.Term, "setting to -1 (1)")
		rf.votedFor = -1
		rf.persist()
	}
	reply.Term = rf.currentTerm
	// TODO need to incorporate indexes 2D
	LastLogIndex, LastLogTerm := rf.findLastLogIndexAndTerm()
	//log.Println("Server ", rf.me, " last log index is ", LastLogIndex)
	//log.Println("Server ", rf.me, " last log term is ", LastLogTerm)
	//log.Println("Server ", rf.me, " last included index is ", rf.lastIncludedIndex)
	//log.Println("Server ", rf.me, " last included term is ", rf.lastIncludedTerm)
	//log.Println("Server ", rf.me, " arg last log index is ", args.LastLogIndex)
	//log.Println("Server ", rf.me, " arg last log term is ", args.LastLogTerm)
	if ((rf.votedFor == -1) || (rf.votedFor == args.CandidateId)) &&
		((len(rf.log)+rf.lastIncludedIndex == 0) ||
			(args.LastLogTerm > LastLogTerm || ((args.LastLogTerm == LastLogTerm) && (args.LastLogIndex >= LastLogIndex)))) {
		if len(rf.log)+rf.lastIncludedIndex == 0 {
			//log.Println("Server ", rf.me, "len(rf.log)+rf.lastIncludedIndex == 0")
		}
		reply.VoteGranted = true
		//log.Println("Server ", rf.me, " gave vote to ", args.CandidateId)
		rf.votedFor = args.CandidateId
		rf.persist()
	} else {
		reply.VoteGranted = false
		//log.Println("Server ", rf.me, " did not give vote to ", args.CandidateId)
	}
	if len(rf.log) != 0 {
		//log.Println("vote for server ", args.CandidateId, " for term ", args.Term, ", expression was ", ((args.LastLogTerm == rf.log[len(rf.log)-1][1].(int)) && (args.LastLogIndex >= len(rf.log))), " for server ", rf.me)
		//log.Println("vote for server ", args.CandidateId, " for term ", args.Term, ", expression1 was ", (args.LastLogIndex >= len(rf.log)), " for server ", rf.me)
		//log.Println("vote for server ", args.CandidateId, " for term ", args.Term, ", expression2 was ", (args.LastLogTerm == rf.log[len(rf.log)-1][1].(int)), " for server ", rf.me)
		//log.Println("lastLogTerm is ", args.LastLogTerm, " for server ", rf.me)
		//log.Println("lastLogIndex is ", args.LastLogIndex, " for server ", rf.me)
		//// //log.Println(rf.log[len(rf.log)-1][1].(int))
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	// Your code here (2B).
	rf.mu.Lock()
	// //log.Println(rf.me, "locked")
	// defer //log.Println(rf.me, "unlocked")
	defer rf.mu.Unlock()
	if rf.isLeader {
		//// //log.Println("Leader is ", rf.me)
		// index = rf.commitIndex + 1
		index = rf.lastIncludedIndex + len(rf.log) + 1
		term = rf.currentTerm
		//log.Println("Leader Server ", rf.me, " is appending log entry ", [2]interface{}{command, term})
		rf.log = append(rf.log, [2]interface{}{command, term})
		rf.persist()
		////log.Printf("Server %d: Appended log entry as Leader %d\n", rf.me, [2]interface{}{command, term})
		// take the necessary actions to replicate the log entry on a majority of servers
		//log.Println("sending from 1 because ", rf.isLeader)
		rf.sendMissingEntriesToAll()
		// for {
		// 	applied := <-rf.applyCh
		//// //log.Println("applied is ", applied)
		//// //log.Println("index is ", index)
		// 	if applied.CommandIndex == index {
		// 		break
		// 	}
		// }
	}

	return index, term, rf.isLeader // TODO
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) findLastLogIndexAndTerm() (int, int) {
	if len(rf.log) > 0 {
		return len(rf.log) + rf.lastIncludedIndex, rf.log[len(rf.log)-1][1].(int)
	}
	return rf.lastIncludedIndex, rf.lastIncludedTerm
}

func (rf *Raft) checkIfCanApply() {
	// need to be locked when performing
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			//log.Println("Apply Server", rf.me, "rf.lastapplied is ", rf.lastApplied, "rf.commitindex is ", rf.commitIndex, "i is ", i, "len(rf.log) is ", len(rf.log), "rf.lastincludedindex is ", rf.lastIncludedIndex)
			if i < rf.lastIncludedIndex+1 {
				// set lastincludedindex to inf
				// TODO need to get a snapshot
				break
			}
			if i > len(rf.log)+rf.lastIncludedIndex {
				break
			}
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i-rf.lastIncludedIndex-1][0],
				CommandIndex: i,
			}
			//log.Printf("(1) Server %d: Applying log entry number %d which is %d\n", rf.me, i, rf.log[i-rf.lastIncludedIndex-1][0])
			rf.lastApplied = i
			rf.applyMsgCh <- applyMsg
		}
	}
}

func (rf *Raft) ticker() {
	ms := 50 + (rand.Int63() % 300)
	time.Sleep(time.Duration(ms) * time.Millisecond)
	for !rf.killed() {
		rf.mu.Lock()
		rf.checkIfCanApply()
		// 		If commitIndex > lastApplied: increment lastApplied, apply
		// log[lastApplied] to state machine (§5.3)
		// if rf.commitIndex > rf.lastApplied {
		// 	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		// 		if i > len(rf.log)+rf.lastIncludedIndex {
		// 			break
		// 		}
		// 		applyMsg := ApplyMsg{
		// 			CommandValid: true,
		// 			Command:      rf.log[i-rf.lastIncludedIndex-1][0],
		// 			CommandIndex: i,
		// 		}
		//// 		//log.Printf("(1) Server %d: Applying log entry number %d which is %d\n", rf.me, i, rf.log[i-rf.lastIncludedIndex-1][0])
		// 		rf.applyMsgCh <- applyMsg
		// 	}
		// 	// rf.lastApplied = rf.commitIndex
		// }

		if !rf.isLeader && time.Since(rf.lastHeartbeat) > ElectionTimeout {
			voteCount := 1
			rf.currentTerm++
			//log.Printf("Server %d: Starting election for term %d\n", rf.me, rf.currentTerm)
			rf.votedFor = rf.me
			rf.persist()

			for i := range rf.peers {
				if i != rf.me {
					go func(i int, currentTerm int) {
						rf.mu.Lock()
						LastLogIndex, LastLogTerm := rf.findLastLogIndexAndTerm()
						args := &RequestVoteArgs{
							Term:         currentTerm,
							CandidateId:  rf.me,
							LastLogIndex: LastLogIndex,
							LastLogTerm:  LastLogTerm,
						}
						rf.mu.Unlock()
						reply := &RequestVoteReply{}
						call := rf.sendRequestVote(i, args, reply)
						if call && reply.VoteGranted {
							rf.mu.Lock()
							if rf.currentTerm == currentTerm {
								voteCount++
							}
							////log.Printf("Server %d: Received vote from server %d for term %d\n", rf.me, i, args.Term)
							if voteCount > len(rf.peers)/2 && !rf.isLeader {
								rf.isLeader = true
								// log.Printf("Server %d: Became leader for term %d\n", rf.me, rf.currentTerm)
								//log.Println("log is ", rf.log, " on leader server ", rf.me)
								rf.sendMissingEntriesToAll()
								// rf.sendHeartbeatToAll()
								rf.matchIndex = make([]int, len(rf.peers))
								rf.nextIndex = make([]int, len(rf.peers)) // initialized to leader last log index + 1
								for i := range rf.peers {
									rf.nextIndex[i] = rf.lastIncludedIndex + len(rf.log) + 1
								}
							}
							rf.mu.Unlock()
						}
					}(i, rf.currentTerm)
				}
			}
		}
		rf.mu.Unlock()

		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      [][2]interface{}
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	Rejection Rejection
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//log.Println("Server", rf.me, "args.Term is ", args.Term)
	//log.Println("Server", rf.me, "rf.currentTerm is ", rf.currentTerm)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		//log.Println("server ", rf.me, " is returning because term is ", args.Term, " and current term is ", rf.currentTerm)
		reply.Success = false
		return
	}
	rf.lastHeartbeat = time.Now()
	rf.currentTerm = args.Term
	if rf.isLeader {
		//log.Println("Server ", rf.me, "removing myself as leader because got a heartbeat from server ", args.LeaderId, " with term ", args.Term)
	}
	rf.isLeader = false
	// rf.votedFor = -1
	rf.persist()

	if rf.lastApplied < rf.lastIncludedIndex {
		// send Rejection such that it will set nextindex to zero
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.Rejection = Rejection{
			XTerm: -1,
		}
		return
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	// XTerm:  term in the conflicting entry (if any)
	// XIndex: index of first entry with that term (if any)
	// XLen:   log length
	if !(args.PrevLogIndex-rf.lastIncludedIndex < 1) && (args.PrevLogIndex-rf.lastIncludedIndex > len(rf.log) || rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1][1].(int) != args.PrevLogTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
		xTerm := 0
		// if len(rf.log) > 0 && args.PrevLogIndex-rf.lastIncludedIndex > len(rf.log) {
		// 	xTerm = rf.log[len(rf.log)-1][1].(int)
		// } else
		if args.PrevLogIndex-rf.lastIncludedIndex < len(rf.log) && args.PrevLogIndex-rf.lastIncludedIndex > 0 && rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1][1].(int) != args.PrevLogTerm {
			xTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1][1].(int)
		}
		//log.Println("Server ", rf.me, " log is ", rf.log)
		//log.Println("Server ", rf.me, " xTerm is ", xTerm)
		//log.Println("Server ", rf.me, " XIndex is ", rf.firstIndexForTerm(xTerm))
		reply.Rejection = Rejection{
			XTerm:  xTerm,
			XIndex: rf.firstIndexForTerm(xTerm),
			XLen:   len(rf.log) + rf.lastIncludedIndex,
		}
		// reply.Rejection = Rejection{
		// 	XTerm:  rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1][1].(int),
		// 	XIndex: args.PrevLogIndex-rf.lastIncludedIndex,
		// 	XLen:   len(rf.log),
		// }
		// if args.Entries != nil {
		//// //log.Println("entries is ", args.Entries)
		// }
		//// //log.Println("prevLogIndex is ", args.PrevLogIndex-rf.lastIncludedIndex)
		//// //log.Println("prevLogTerm is ", args.PrevLogTerm)
		//// //log.Println("log is ", rf.log)
		//// //log.Println(!(args.PrevLogIndex-rf.lastIncludedIndex == 0))
		//// //log.Println(args.PrevLogIndex-rf.lastIncludedIndex > len(rf.log))
		//// //log.Println(rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1][1].(int) != args.PrevLogTerm)
		return
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	//log.Println("PrevLogIndex is ", args.PrevLogIndex-rf.lastIncludedIndex, "for server ", rf.me)
	//log.Println("PrevLogTerm is ", args.PrevLogTerm, "for server ", rf.me)
	//log.Println("Entries is ", args.Entries)
	//log.Println("LeaderId is ", args.LeaderId)
	cutoff := len(rf.log)
	// for i := args.PrevLogIndex-rf.lastIncludedIndex; i < len(rf.log) && i < args.PrevLogIndex-rf.lastIncludedIndex+len(args.Entries); i++ {
	// 	if rf.log[i][1].(int) != args.PrevLogTerm {
	// 		rf.log = rf.log[:i]
	// 		break
	// 	}
	// 	cutoff = i
	// }
	for i, entry := range args.Entries {
		logIndex := args.PrevLogIndex - rf.lastIncludedIndex + i
		if logIndex < len(rf.log) && logIndex >= 0 && rf.log[logIndex][1].(int) != entry[1].(int) {
			rf.log = rf.log[:logIndex]
			rf.persist()
			cutoff = logIndex
			break
		}
		cutoff = logIndex + 1
	}
	if len(rf.log) < cutoff {
		//log.Println("cutoff is ", cutoff)
	}
	// rf.log = rf.log[:cutoff]

	// for each entry in args that is not already in log

	alreadyInLog := false
	for index, entry := range args.Entries {
		// Check if the entry is already in the log
		// TODO FIX THIS?
		alreadyInLog = false
		if args.PrevLogIndex-rf.lastIncludedIndex+index < 0 {
			//log.Println("args.PrevLogIndex-rf.lastIncludedIndex+index < 0")
			alreadyInLog = true
		} else if args.PrevLogIndex-rf.lastIncludedIndex+index < len(rf.log) {
			if rf.log[args.PrevLogIndex-rf.lastIncludedIndex+index][1].(int) == entry[1].(int) {
				alreadyInLog = true
				//log.Println("Server ", rf.me, " already has log entry ", entry)
				//log.Println("Server ", rf.me, " log is ", rf.log)
			}
		}
		// If the entry is not in the log, append it
		if !alreadyInLog {
			//log.Println("Server ", rf.me, " is appending log entry ", entry)
			rf.log = append(rf.log, entry)
			rf.persist()
			//log.Println("Server ", rf.me, " now has log: ", rf.log)
		}
		//// //log.Println("log is ", rf.log)
	}
	// if (args.LeaderCommit > rf.commitIndex) && (len(args.Entries) > 0) {
	// if args.LeaderCommit > rf.commitIndex {
	// 	// send newly committed entries on applyCh
	// 	// If leaderCommit > commitIndex, set commitIndex =
	// 	// min(leaderCommit, index of last new entry)

	//// //log.Println("log is ", rf.log, " on server ", rf.me)
	//// //log.Println("commitIndex is ", rf.commitIndex, "on server ", rf.me)
	//// //log.Println("leadercommit is ", args.LeaderCommit)
	// 	for i := rf.commitIndex; i < len(rf.log)+rf.lastIncludedIndex; i++ {
	// 		if i >= args.LeaderCommit {
	// 			break
	// 		}
	// 		applyMsg := ApplyMsg{
	// 			CommandValid: true,
	// 			Command:      rf.log[i-rf.lastIncludedIndex][0],
	// 			CommandIndex: i + 1,
	// 		}
	//// 		//log.Printf("Server %d: Applying log entry number %d which is %d\n", rf.me, i+1, rf.log[i-rf.lastIncludedIndex][0])
	// 		rf.applyMsgCh <- applyMsg
	// 		// rf.lastApplied = i + 1
	// 	}
	//log.Println("commitIndex is ", rf.commitIndex)
	// }
	rf.commitIndex = args.LeaderCommit
	//log.Println("Commit index is", rf.commitIndex, "on server ", rf.me)
	//log.Println("lastapplied is", rf.lastApplied, "on server", rf.me)

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) findLastIndexForTerm(term int) (int, bool) {
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i][1] == term {
			return rf.lastIncludedIndex + i + 1, true
		}
	}
	return -1, false
}

func (rf *Raft) firstIndexForTerm(term int) int {
	for i, logEntry := range rf.log {
		if logEntry[1] == term {
			return rf.lastIncludedIndex + i + 1
		}
	}
	return -1
}

func (rf *Raft) sendAppendEntries(server int, originalInvocationTime time.Time, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//log.Println("Server", rf.me, "sending appendentries to server", server, "with term", args.Term)
	//// //log.Println("Server", rf.me, "sending AE to", server)
	//// //log.Println(args.PrevLogIndex, len(args.Entries))
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if (reply.Term > args.Term) || (reply.Term > rf.currentTerm) {
			//log.Println("removing myself as leader because got a reply from server ", server, " with term ", reply.Term)
			//log.Println("But checking if I can commit something first")
			rf.checkCommitIndex()
			rf.currentTerm = reply.Term
			rf.isLeader = false
			rf.votedFor = -1
			rf.persist()
			return ok
		}
		if reply.Success {
			// if Snapshot() has been called in the meantime then it will be out of date
			// we can check if Snapshot() has been called by checking if the commitindex has been decremented since sending the request
			// if len(args.Entries) == 0 || rf.commitIndex < args.LeaderCommit {
			//// //log.Println("len(entries) is", len(args.Entries))
			//// //log.Println("rf.commitindex is", rf.commitIndex)
			//// //log.Println("args.LeaderCommit is", args.LeaderCommit)
			// 	return ok
			// }
			//log.Println("Server ", rf.me, " lastincludedindex is ", rf.lastIncludedIndex)
			//log.Println("Server ", rf.me, " prevlogindex is ", args.PrevLogIndex)
			//log.Println("updating nextindex from ", rf.nextIndex[server], " to ", args.PrevLogIndex+len(args.Entries)+1, " for server ", server)
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			//log.Println("updating matchindex from ", rf.matchIndex[server], " to ", rf.nextIndex[server]-1)
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			return ok
		} else {
			if reply.Rejection.XTerm == -1 {
				//// //log.Println("server", rf.me, "updating nextindex from ", rf.nextIndex[server], " to ", 0, " for server ", server)
				rf.nextIndex[server] = 0
			}
			if reply.Rejection.XTerm != 0 {
				if i, ok := rf.findLastIndexForTerm(reply.Rejection.XTerm); ok {
					//log.Println("i is ", i)
					//log.Println("Xterm is ", reply.Rejection.XTerm)
					//log.Println("server", rf.me, "log is", rf.log)
					if rf.nextIndex[server] > i+1 {
						//// //log.Println("server", rf.me, "(1.2) updating nextindex from ", rf.nextIndex[server], " to ", i+1, " for server ", server)
						rf.nextIndex[server] = i + 1
					} else {
						//// //log.Println("server", rf.me, "(1.1) updating nextindex from ", rf.nextIndex[server], " to ", rf.nextIndex[server]-1, " for server ", server)
						rf.nextIndex[server]--
					}
				} else {
					//// //log.Println("server", rf.me, "(2) updating nextindex from ", rf.nextIndex[server], " to ", reply.Rejection.XIndex, " for server ", server)
					rf.nextIndex[server] = reply.Rejection.XIndex
				}
			} else if reply.Rejection.XLen < len(rf.log) {
				//// //log.Println("server", rf.me, "(3) updating nextindex from ", rf.nextIndex[server], " to ", int(math.Min(float64(reply.Rejection.XLen), float64(rf.nextIndex[server]-1))), " for server ", server)
				rf.nextIndex[server] = int(math.Max(math.Min(float64(reply.Rejection.XLen), float64(rf.nextIndex[server]-1)), 1.0))
			} else {
				//// //log.Println("server", rf.me, "(4) updating nextindex from ", rf.nextIndex[server], " to ", rf.nextIndex[server]-1, " for server ", server)
				rf.nextIndex[server]--
			}
		}
	} else {
		//// //log.Println("call from server ", args.LeaderId, " to server ", server, " failed with entries ", args.Entries)
	}
	if len(args.Entries) > 0 && time.Since(originalInvocationTime) < HeartbeatInterval {
		go rf.sendMissingEntries(server, originalInvocationTime)
	}

	return ok
}

// func (rf *Raft) sendHeartbeatToAll() {
// 	for i := range rf.peers {
// 		if i != rf.me {
// 			go func(i int) {
// 				rf.mu.Lock()
// //log.Println(rf.me, "locked")
// 				args := &AppendEntriesArgs{
// 					Term:     rf.currentTerm,
// 					LeaderId: rf.me,
// 					// LeaderCommit: rf.commitIndex,
// 				}
// 				rf.mu.Unlock()
// //log.Println(rf.me, "unlocked")
// 				reply := &AppendEntriesReply{}
// 				rf.sendAppendEntries(i, args, reply)
// 			}(i)
// 		}
// 	}
// }

func (rf *Raft) sendMissingEntries(i int, originalInvocationTime time.Time) {
	if time.Since(originalInvocationTime) > HeartbeatInterval && originalInvocationTime != (time.Time{}) {
		//log.Println("Server ", rf.me, " is taking too long to send missing entries to server ", i)
		return
	}

	rf.mu.Lock()
	if !rf.isLeader {
		//log.Println("Server ", rf.me, " is not leader anyways")
		rf.mu.Unlock()
		return
	}
	//log.Println("Server ", rf.me, " sending missing entries to server ", i)
	entries := make([][2]interface{}, 0)
	// send all the entries that are not in the log of the follower
	if rf.nextIndex[i]-1-rf.lastIncludedIndex < 0 {
		rf.mu.Unlock()
		//// //log.Println(rf.me, "sending installsnapshot", i)
		go rf.sendInstallSnapshot(i)
		return
	} else {
		for j := rf.nextIndex[i] - 1; j < len(rf.log)+rf.lastIncludedIndex; j++ {
			entries = append(entries, rf.log[j-rf.lastIncludedIndex])
		}
	}

	PrevLogTerm := 0
	//log.Println("Server ", rf.me, " nextIndex is ", rf.nextIndex)

	if rf.nextIndex[i]-rf.lastIncludedIndex > 1 && rf.nextIndex[i]-rf.lastIncludedIndex-2 <= len(rf.log)-1 {
		PrevLogTerm = rf.log[rf.nextIndex[i]-rf.lastIncludedIndex-2][1].(int)
	} else {
		PrevLogTerm = rf.lastIncludedTerm
	}

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[i] - 1,
		PrevLogTerm:  PrevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	if len(entries) > 0 {
		//log.Println("Server ", rf.me, ": Sending missing entries ", entries, " to server ", i)
		//log.Println("PrevLogTerm is", args.PrevLogTerm)
		//log.Println("PrevLogIndex is", args.PrevLogIndex)
	}

	reply := &AppendEntriesReply{}
	if originalInvocationTime == (time.Time{}) {
		originalInvocationTime = time.Now()
	}
	go rf.sendAppendEntries(i, originalInvocationTime, args, reply)
}

func (rf *Raft) sendMissingEntriesToAll() {
	for i := range rf.peers {
		if i != rf.me {
			//// //log.Println("sending to ", i)
			go rf.sendMissingEntries(i, time.Time{})
		}
	}
	rf.checkCommitIndex()
}

func (rf *Raft) checkCommitIndex() {
	if rf.commitIndex < rf.lastIncludedIndex {
		rf.checkIfCanApply()
		return
	}
	for i := rf.commitIndex; i < len(rf.log)+rf.lastIncludedIndex; i++ {
		count := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] > i && rf.log[i-rf.lastIncludedIndex][1].(int) == rf.currentTerm {
				count++
			}
		}
		//log.Println("matchindex is", rf.matchIndex)
		//log.Println("i is", i)
		//log.Println("count is", count)
		//log.Println("length of peers is", len(rf.peers))
		// if count <= len(rf.peers)/2 {
		// 	break
		// }
		if count > len(rf.peers)/2 {
			rf.commitIndex = i + 1
			// for k := rf.commitIndex; k <= i; k++ {
			// 	applyMsg := ApplyMsg{
			// 		CommandValid: true,
			// 		Command:      rf.log[k-rf.lastIncludedIndex][0],
			// 		CommandIndex: k + 1,
			// 	}
			//// 	//log.Printf("Leader server %d: Applying log entry number %d which is %d\n", rf.me, k+1, rf.log[k-rf.lastIncludedIndex][0])
			//// //log.Println("rf.lastincludedindex is", rf.lastIncludedIndex)
			//// //log.Println("k is", k)
			//// //log.Println(string(debug.Stack()))
			// 	rf.applyMsgCh <- applyMsg
			// 	// rf.lastApplied = k + 1
			// 	// rf.persist()
			// }
			//log.Println("Commit index is", rf.commitIndex)
		}
	}

	rf.checkIfCanApply()
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// log.SetOutput(io.Discard)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.isLeader = false

	rf.log = make([][2]interface{}, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyCh = applyCh

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.applyMsgCh = make(chan ApplyMsg, 1500) // Buffer size can be adjusted as needed

	// Start a goroutine to send apply messages
	go func() {
		for applyMsg := range rf.applyMsgCh {
			//log.Println("Server ", rf.me, " is trying to send ", applyMsg)
			rf.applyCh <- applyMsg
			// //log.Println("waiting for lock")
			// if rf.lastIncludedIndex != 0 {
			// 	rf.mu.Lock()
			// 	// //log.Println(rf.me, "locked")
			// 	if applyMsg.CommandIndex == rf.lastApplied+1 {
			// 		rf.lastApplied++
			// 	}
			// 	rf.mu.Unlock()
			// 	// //log.Println(rf.me, "unlocked")
			// }
			//log.Println("Server ", rf.me, " sent ", applyMsg)
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start a goroutine that sends heartbeats periodically if this server is the leader
	go func() {
		for !rf.killed() {
			rf.mu.Lock()

			if rf.isLeader {
				//log.Println("sending from 2 because ", rf.isLeader, rf.me)
				rf.sendMissingEntriesToAll()
				// rf.sendHeartbeatToAll()
				// check if any new log entries can be committed
				//// //log.Println("commitindex is ", rf.commitIndex)
			}
			rf.mu.Unlock()
			time.Sleep(HeartbeatInterval)
		}
	}()

	return rf
}
