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
	//	"bytes"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state for all server
	commitIndex int // index of highest log entry known to be commit
	lastApplied int // index of highest log entry applied to state machine (lastApplied < commitIndex)

	// volatile state for leader
	// these fields are reinitialized after leader election
	nextIndex  []int // index of next log entry send to each server, initialized to length of peers
	matchIndex []int // index of highest log entry known to be replicated on server

	lastAppendEntriesTime time.Time

	// 3B
	applyCh chan ApplyMsg // leader will commit msg to this channel

	// use to count broadcast // 3A (test usage)
	broadcastCount int32

	replicaProcess []int
	// commitCh       chan int // channel to count replicated client
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.votedFor == rf.me
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
// goroutine should hold the lock while invoke this function
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	fmt.Printf("node %v write term %v vote %v log %v\n", rf.me, rf.currentTerm, rf.votedFor, rf.log)
	// e.Encode(rf.commitIndex)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	// var commitIndex int

	// if d.Decode(&currentTerm) == nil &&
	// 	d.Decode(&votedFor) == nil &&
	// 	d.Decode(&log) == nil &&
	// 	d.Decode(&commitIndex) == nil {
	// 	rf.currentTerm = currentTerm
	// 	rf.votedFor = votedFor
	// 	rf.log = log
	// 	fmt.Printf("node %v read term %v vote %v commitIndex %v log %v\n", rf.me, currentTerm, votedFor, commitIndex, log)
	// 	rf.commitIndex = commitIndex
	// 	for i := 0; i < len(rf.peers); i++ {
	// 		if i == rf.me {
	// 			continue
	// 		}
	// 		rf.nextIndex[i] = rf.commitIndex + 1
	// 	}
	// }

	if d.Decode(&currentTerm) == nil &&
		d.Decode(&votedFor) == nil &&
		d.Decode(&log) == nil {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		fmt.Printf("node %v read term %v vote %v log %v\n", rf.me, currentTerm, votedFor, log)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			rf.nextIndex[i] = rf.commitIndex + 1
		}
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// leader could send multiple entries within one AppednEntries RPC
type AppendEntriesArgs struct {
	Term         int // leader term
	LeaderId     int
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of PrevLogIndex
	Entries      []LogEntry // log entries to store (empty for heartbeat, may send many for efficiency)
	LeaderCommit int        // leader commit index
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Index   int // index of the last commited log entry
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("node %d get request vote from %d term %v current term %v commitIndex %v\n", rf.me, args.CandidateId, args.Term, rf.currentTerm, args.LastLogIndex)

	// candidate's term is less than current term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// candidate's log should be at least as up-to-date as log for majority of servers
	if args.LastLogIndex < rf.commitIndex || (rf.commitIndex >= 0 && args.LastLogTerm < rf.log[rf.commitIndex].Term) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// current server has smaller term -> transfer to follower
	if args.Term > rf.currentTerm {
		reply.Term = args.Term
		rf.currentTerm = args.Term
		if rf.votedFor == rf.me {
			fmt.Printf("leader %d transfer to follower\n", rf.me)
		}
		rf.votedFor = -1
		// added for 3C
		rf.persist()
	}

	if rf.votedFor == -1 {
		// has not voted
		fmt.Printf("node %d grant request vote from %d term %d\n", rf.me, args.CandidateId, args.Term)
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		// added for 3C
		rf.persist()
		// update last append entries time
		rf.lastAppendEntriesTime = time.Now()
	} else {
		// voted
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

// server will reject AppendEntriesRPC, if PrevLogIndex and PrevLogTerm mismatch
// same index, same term -> same command
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("node %d get append entry from %d current term %d arg term %d rf commit %v commit index %v prev index %v logs %v\n",
		rf.me, args.LeaderId, rf.currentTerm, args.Term, rf.commitIndex, args.LeaderCommit, args.PrevLogIndex, args.Entries)

	reply.Index = rf.commitIndex
	// AppendRPC with stale term -> reject
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// get AppendRPC from current term with different leader -> reject
	if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.LeaderId {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	fmt.Printf("node %d get valid append entry from %d\n", rf.me, args.LeaderId)

	// get append entry from another leader (with term at least as large as current term)
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	// added for 3C
	rf.persist()

	now := time.Now()
	if now.After(rf.lastAppendEntriesTime) {
		rf.lastAppendEntriesTime = now
	}

	// prev log mismatch
	if len(rf.log) <= args.PrevLogIndex ||
		(args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		if len(rf.log) <= args.PrevLogIndex {
			fmt.Printf("node %d reject append entry from %d log length %v PrevLogIndex %v\n", rf.me, args.LeaderId, len(rf.log), args.PrevLogIndex)
		} else {
			fmt.Printf("node %d reject append entry from %d log term %v PrevLogTerm %v\n", rf.me, args.LeaderId, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		}
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// append log entries
	if len(args.Entries) > 0 {
		i := 0
		j := args.PrevLogIndex + 1
		// same index, term -> same command
		// jump through same command
		for i < len(args.Entries) && j < len(rf.log) {
			if args.Entries[i].Term != rf.log[j].Term {
				break
			}
			i++
			j++
		}
		fmt.Printf("node %d append log from %v i %v j %v\n", rf.me, args.LeaderId, i, j)
		// append remaining log
		if i < len(args.Entries) {
			rf.log = append(rf.log[:j], args.Entries[i:]...)
			// added for 3C
			rf.persist()
		}
		fmt.Printf("node %d append log from %d log %v\n", rf.me, args.LeaderId, rf.log)
	}

	commitIndex := min(args.LeaderCommit, max(len(rf.log)-1, 0))
	if commitIndex > rf.commitIndex {
		// update commit index, commit the msg to channel
		// commit from rf.commitIndex to min(args.LeaderCommit, len(rf.log)-1)
		for i := rf.commitIndex + 1; i <= commitIndex; i++ {
			fmt.Printf("node %v commit index %v log %v\n", rf.me, i, rf.log[i])
			// ApplyMsg index start from 1
			msg := ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i + 1}
			rf.applyCh <- msg
		}
		rf.commitIndex = commitIndex
		// rf.persist()
	}

	fmt.Printf("node %v grant append rpc from %v log length %v\n", rf.me, args.LeaderId, len(rf.log))
	reply.Term = rf.currentTerm
	reply.Success = true
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.votedFor == rf.me
	index := 0

	if isLeader {
		entry := LogEntry{Term: rf.currentTerm, Command: command}
		// append log entry to leader
		rf.log = append(rf.log, entry)
		// added for 3C
		rf.persist()
		index = len(rf.log)
		rf.nextIndex[rf.me] = max(rf.nextIndex[rf.me], index+1)
		// applyMsg index start from 1
		fmt.Printf("leader %v append entry %v command %v\n", rf.me, index, command)
		go rf.AppendEntriesRPC(index - 1)
	}

	return index, term, isLeader
}

// this function should be invoked by leader only
// append log entry at @param: logIndex to all server
// logIndex can be -1 to indicate current AppendEntriesRPC is a headerbeat
func (rf *Raft) AppendEntriesRPC(logIndex int) {
	rf.mu.Lock()
	if rf.votedFor != rf.me {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	appendChan := make(chan bool)
	rejectChan := make(chan bool)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.AppendEntriesRPCToServer(i, logIndex, appendChan, rejectChan)
		}
	}

	appendFailCount := 0
	count := 0

	for {
		select {
		case msg := <-appendChan:
			if !msg {
				appendFailCount++
				// cannot connect to majority of server: lost leadership
				if appendFailCount<<1 > len(rf.peers) {
					fmt.Printf("leader %v append entires %v lost leadership\n", rf.me, logIndex)
					rf.mu.Lock()
					rf.votedFor = -1
					rf.persist()
					rf.lastAppendEntriesTime = time.Now()
					rf.mu.Unlock()
					return
				}
			}
			count++
			if count == len(rf.peers)-1 {
				return
			}
		case msg := <-rejectChan:
			if msg {
				return
			}
		}
	}
}

func (rf *Raft) AppendEntriesRPCToServer(server int, logIndex int, appendChan chan bool, rejectChan chan bool) {
	for {
		rf.mu.Lock()
		// leader changed
		if rf.votedFor != rf.me {
			rf.mu.Unlock()
			rejectChan <- true
			return
		}

		if logIndex >= 0 {
			// other goroutine handles current log replication
			if rf.replicaProcess[server] > logIndex {
				rf.mu.Unlock()
				// true to append channel does not means append success
				appendChan <- true
				return
			}
			rf.replicaProcess[server] = logIndex
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			LeaderCommit: rf.commitIndex,
		}

		if args.PrevLogIndex >= 0 {
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		} else {
			args.PrevLogTerm = -1
		}

		// slice end (log end for heartbeat, log index for AppendLogEntries)
		end := 0
		if logIndex >= 0 {
			end = logIndex
		} else {
			// log index -1 -> heartbeat
			end = len(rf.log) - 1
		}

		args.Entries = rf.log[rf.nextIndex[server] : end+1]
		reply := AppendEntriesReply{}

		rf.mu.Unlock()
		resultChan := make(chan bool)
		go func() {
			fmt.Printf("leader %v append log index %v to server %v prev index %v\n",
				rf.me, logIndex, server, args.PrevLogIndex)
			resultChan <- rf.sendAppendEntries(server, &args, &reply)
		}()

		select {
		case <-time.After(300 * time.Millisecond):
			fmt.Printf("leader %v append log index %v to server %v timeout\n", rf.me, logIndex, server)
			appendChan <- false
			return
		case rst := <-resultChan:
			if !rst {
				fmt.Printf("leader %v append log index %v to server %v connection fail\n", rf.me, logIndex, server)
				appendChan <- false
				return
			}
			rf.mu.Lock()
			// lost leadership
			if rf.votedFor != rf.me {
				rf.mu.Unlock()
				rejectChan <- true
				return
			}
			if reply.Success {
				rf.nextIndex[server] = max(rf.nextIndex[server], end+1)
				fmt.Printf("leader %v append log index %v to server %v success\n", rf.me, logIndex, server)
				appendChan <- true
				rf.mu.Unlock()
				rf.CheckLogCommitment(end)
				return
			} else {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					// added for 3C
					rf.persist()
					fmt.Printf("leader %v append log index %v to server %v fail, lost leadership\n", rf.me, logIndex, server)
					rf.lastAppendEntriesTime = time.Now()
					rejectChan <- true
					rf.mu.Unlock()
					return
				}

				fmt.Printf("leader %v append log index %v to server %v fail, next index fallback to %v\n", rf.me, logIndex, server, reply.Index+1)
				rf.nextIndex[server] = min(rf.nextIndex[server], reply.Index+1)
				rf.mu.Unlock()
				// sleep before retry
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

// func (rf *Raft) LeaderCommitChecker() {
// 	record := make(map[int]int, 0)
// 	for logIndex := range rf.commitCh {
// 		rf.mu.Lock()
// 		if rf.commitIndex >= logIndex {
// 			rf.mu.Unlock()
// 			continue
// 		}
// 		rf.mu.Unlock()

// 		// commit to logIndex indicates commit to all entries below
// 		for key := range record {
// 			if key <= logIndex {
// 				record[key]++
// 			}
// 		}

// 		max := -1

// 		for key := range record {
// 			if record[key]<<1 > len(rf.peers) && key > max {
// 				max = key
// 			}
// 		}

// 		// has entries to commit
// 		if max >= 0 {
// 			rf.mu.Lock()
// 			if rf.commitIndex < max {
// 				for i := rf.commitIndex; i <= max; i++ {
// 					fmt.Printf("leader %v commit index %v log %v\n", rf.me, i, rf.log[i])
// 					// ApplyMsg index start from 1
// 					msg := ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i + 1}
// 					rf.applyCh <- msg
// 				}
// 				rf.commitIndex = max
// 			}
// 			rf.mu.Unlock()

// 			// free space for record
// 			for key := range record {
// 				if key <= max {
// 					delete(record, key)
// 				}
// 			}
// 		}
// 	}
// }

// check if current log can be commited
func (rf *Raft) CheckLogCommitment(logIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// current server is not leader
	if rf.votedFor != rf.me {
		return
	}

	// current server has commited current log
	if rf.commitIndex >= logIndex {
		return
	}
	i := logIndex
	// find the largest log index that can be commited
	for i > rf.commitIndex {
		vote := 1
		for j := 0; j < len(rf.peers); j++ {
			if j == rf.me {
				continue
			}
			if rf.nextIndex[j] > logIndex {
				vote++
			}
		}
		// get majority
		if vote<<1 > len(rf.peers) {
			break
		}
		i--
	}

	// raft does not commit previous log
	if i >= 0 && rf.log[i].Term < rf.currentTerm {
		return
	}

	// commit until i
	for j := rf.commitIndex + 1; j <= i; j++ {
		fmt.Printf("leader %v commit index %v log %v\n", rf.me, j, rf.log[j])
		// ApplyMsg index start from 1
		msg := ApplyMsg{CommandValid: true, Command: rf.log[j].Command, CommandIndex: j + 1}
		rf.applyCh <- msg
	}

	rf.commitIndex = i
	// rf.persist()
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

func (rf *Raft) RequestVoteRPCToServer(server int, term int, voteChan chan bool, rejectChan chan bool) {
	rf.mu.Lock()
	if term != rf.currentTerm || rf.votedFor != -1 {
		rf.mu.Unlock()
		rejectChan <- true
		return
	}
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: rf.commitIndex,
	}
	if args.LastLogIndex >= 0 {
		args.LastLogTerm = rf.log[args.LastLogIndex].Term
	}
	reply := RequestVoteReply{}
	rf.mu.Unlock()

	rstChan := make(chan bool)
	go func() {
		rstChan <- rf.sendRequestVote(server, &args, &reply)
	}()

	select {
	case <-time.After(300 * time.Millisecond):
		fmt.Printf("node %d request vote RPC to %d timeout\n", rf.me, server)
		voteChan <- false
	case rst := <-rstChan:
		if rst && reply.VoteGranted {
			fmt.Printf("node %d get vote from %d\n", rf.me, server)
			voteChan <- true
		} else {
			// other reject the vote
			if rst && reply.Term > term {
				rf.mu.Lock()
				rf.currentTerm = max(rf.currentTerm, reply.Term)
				rf.votedFor = -1
				// added for 3C
				rf.persist()
				rf.mu.Unlock()
				rejectChan <- true
				fmt.Printf("node %d get larger term vote from %d\n", rf.me, server)
			} else {
				voteChan <- false
				fmt.Printf("node %d get reject vote from %d\n", rf.me, server)
			}
		}
	}
}

// start an election
func (rf *Raft) RequestVoteRPC() bool {
	rf.mu.Lock()
	// already vote for other server as leader
	if rf.votedFor != -1 {
		rf.mu.Unlock()
		return false
	}
	term := rf.currentTerm
	rf.mu.Unlock()

	fmt.Printf("node %d start leader election\n", rf.me)
	voteCh := make(chan bool)
	rejectChan := make(chan bool)

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.RequestVoteRPCToServer(i, term, voteCh, rejectChan)
		}
	}

	voteCount := 0
	count := 0
loop:
	for {
		select {
		case msg := <-rejectChan:
			if msg {
				return false
			}
		case msg := <-voteCh:
			if msg {
				voteCount++
				if (voteCount+1)<<1 > len(rf.peers) {
					break loop
				}
			}
			count++
			if count == len(rf.peers)-1 {
				break loop
			}
		}
	}

	fmt.Printf("node %v get vote %v\n", rf.me, voteCount)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor == -1 && rf.currentTerm == term && ((voteCount+1)<<1 > len(rf.peers))
}

// for 3A args can be empty -> just to update lastAppendEntriesTime
func (rf *Raft) broadcastHeartbeat() {
	atomic.AddInt32(&rf.broadcastCount, 1)
	fmt.Printf("leader %v broadcast heartbeat %d\n", rf.me, atomic.LoadInt32(&rf.broadcastCount))
	// broadcast in current routine
	// rf.AppendEntriesRPC(-1)
	go rf.AppendEntriesRPC(-1)
}

// ticker is used for check leader heartbeat (fellower) and broadcast heartbeat (leader)
func (rf *Raft) ticker() {
	// Your code here (3A)
	for !rf.killed() {
		// lab requires no more than 10 times heartbeat per second
		ms := 300
		// var ms int64
		rf.mu.Lock()
		if rf.votedFor != rf.me {
			// pause for a random amount of time between 300 and 450 ms
			ms = 300 + int(rand.Int63()%150)
		}
		rf.mu.Unlock()

		// milliseconds.
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()
		if rf.votedFor == rf.me {
			rf.mu.Unlock()
			rf.broadcastHeartbeat()
		} else {
			currentTime := time.Now()
			// Check if a leader election should be started.
			// If this peer hasn't heard from a leader in a while, it should start an election.
			duration := currentTime.Sub(rf.lastAppendEntriesTime)
			// rf.mu.Unlock()
			if duration > time.Duration(1000)*time.Millisecond {
				fmt.Printf("node %d duration %v\n", rf.me, duration)
				rf.currentTerm++
				rf.votedFor = -1
				// added for 3C
				rf.persist()
				rf.mu.Unlock()
				rst := rf.RequestVoteRPC()
				for !rst {
					// pause for a random amount of time (in case of split vote)
					ms = int(rand.Int63() % 300)
					time.Sleep(time.Duration(ms) * time.Millisecond)
					rf.mu.Lock()
					if rf.votedFor != -1 {
						// vote for other leader
						rf.mu.Unlock()
						break
					}
					rf.currentTerm++
					rf.persist()
					rf.mu.Unlock()
					rst = rf.RequestVoteRPC()
				}
				rf.mu.Lock()
				// win the election
				if rf.votedFor == -1 {
					rf.votedFor = rf.me
					// added for 3C
					rf.persist()
					for i := 0; i < len(rf.peers); i++ {
						if i == rf.me {
							continue
						}
						// set nextIndex to the current commit index after leader election
						rf.nextIndex[i] = rf.commitIndex + 1
					}
					fmt.Printf("node %d become leader\n", rf.me)
					rf.mu.Unlock()
					// broadcast heartbeat after leader election
					rf.broadcastHeartbeat()
				} else {
					rf.mu.Unlock()
				}
			} else {
				rf.mu.Unlock()
			}
		}
	}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// initialize from state persisted before a crash
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	rf.commitIndex = -1

	// Your initialization code here (3A, 3B, 3C).
	rf.lastApplied = -1

	// 3B does not use matchIndex(?)
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 0
	}

	// rf.matchIndex = make([]int, len(peers))

	rf.lastAppendEntriesTime = time.Now()
	rf.broadcastCount = 0

	rf.replicaProcess = make([]int, len(peers))
	for i := range rf.replicaProcess {
		rf.replicaProcess[i] = -1
	}

	rf.readPersist(persister.ReadRaftState())

	// 3B
	rf.applyCh = applyCh

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
