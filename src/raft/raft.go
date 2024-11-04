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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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

	// update flag is used for heartbeat
	updateFlag []int
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
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("node %d get request vote from %d\n", rf.me, args.CandidateId)

	// candidate's term is less than current term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// current server has smaller term -> transfer to follower
	if args.Term > rf.currentTerm {
		reply.Term = args.Term
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	// candidate's log should be at least as up-to-date as log for majority of servers
	if args.LastLogIndex < rf.commitIndex || (rf.commitIndex >= 0 && args.LastLogTerm < rf.log[rf.commitIndex].Term) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.votedFor == -1 {
		// has not voted
		fmt.Printf("node %d grant request vote from %d term %d\n", rf.me, args.CandidateId, args.Term)
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
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

	fmt.Printf("node %d get append entry from %d current term %d arg term %d rf commit %v commit index %v prev index %v\n",
		rf.me, args.LeaderId, rf.currentTerm, args.Term, rf.commitIndex, args.LeaderCommit, args.PrevLogIndex)

	// AppendRPC with stale term -> reject
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// get AppendRPC from current term with different leader -> reject
	if args.Term == rf.currentTerm && rf.votedFor != args.LeaderId {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	fmt.Printf("node %d get valid append entry from %d\n", rf.me, args.LeaderId)

	// get append entry from another leader (with term at least as large as current term)
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
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
		// append leader log from prevLogIndex + 1
		rf.log = rf.log[:args.PrevLogIndex+1]
		// incase of data race
		for i := 0; i < len(args.Entries); i++ {
			entry := LogEntry{Term: args.Entries[i].Term, Command: args.Entries[i].Command}
			rf.log = append(rf.log, entry)
		}
	}

	commitIndex := min(args.LeaderCommit, max(len(rf.log)-1, 0))
	if commitIndex > rf.commitIndex {
		// update commit index, commit the msg to channel
		// commit from rf.commitIndex to min(args.LeaderCommit, len(rf.log)-1)
		// fmt.Printf("node %v commit log %v\n", rf.me, rf.log[rf.commitIndex+1:commitIndex+1])
		for i := rf.commitIndex + 1; i <= commitIndex; i++ {
			fmt.Printf("node %v commit index %v log %v\n", rf.me, i, rf.log[i])
			// ApplyMsg index start from 1
			msg := ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i + 1}
			rf.applyCh <- msg
		}
		rf.commitIndex = commitIndex
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
		index = len(rf.log)
		// applyMsg index start from 1
		fmt.Printf("leader %v append entry %v command %v\n", rf.me, index, command)
		go rf.AppendEntriesRPC(index - 1)
	}

	return index, term, isLeader
}

// this function should be invoked by leader only
// append log entry at @param: logIndex to all server
// logIndex can be -1 to indicate current AppendEntriesRPC is a headerbeat
func (rf *Raft) AppendEntriesRPC(logIndex int) bool {
	rf.mu.Lock()
	if rf.votedFor != rf.me {
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()

	appendChan := make(chan bool)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.appendEntriesToServer(i, appendChan, logIndex)
		}
	}

	// leader should agree to append log entry
	count := 1
	appendCount := 1

	// appendEntriesToServer will return in reasonable time
	// no need to use select for timeout
	for msg := range appendChan {
		count++
		if msg {
			appendCount++
		}
		if appendCount<<1 > len(rf.peers) {
			break
		}
		if count == len(rf.peers)-1 {
			break
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.votedFor == rf.me {
		// appendEntriesRPC for majority of server
		if appendCount<<1 > len(rf.peers) {
			// commit the entries (one rpc may commit multiple entries)
			if logIndex >= 0 {
				// AppendEntriesRPC run in parallel (update commitIndex)
				for rf.commitIndex < logIndex {
					rf.commitIndex++
					// ApplyMsg index start from 1
					msg := ApplyMsg{CommandValid: true, Command: rf.log[rf.commitIndex].Command, CommandIndex: rf.commitIndex + 1}
					fmt.Printf("leader %v commit log %v command %v\n", rf.me, msg.CommandIndex, msg.Command)
					rf.applyCh <- msg
				}
			}
			return true
		} else {
			// lost leader
			rf.votedFor = -1
			fmt.Printf("node %v lost leader\n", rf.me)
		}
	}

	return false
}

// append log entry to server (heartbeat maybe blocked by updateFlag)
func (rf *Raft) appendEntriesToServer(server int, appendChan chan bool, logIndex int) {
	rf.mu.Lock()
	// heartbeat
	if logIndex == -1 {
		// if updateFlag is set, previous AppendEntriesRPC has not finished, just return
		if rf.updateFlag[server] == 1 {
			fmt.Printf("leader %v heartbeat to server %v blocked\n", rf.me, server)
			rf.mu.Unlock()
			return
		}
	}
	rf.updateFlag[server] = 1
	rf.mu.Unlock()
loop:
	for {
		rf.mu.Lock()
		if rf.votedFor != rf.me {
			// leader has changed
			rf.mu.Unlock()
			appendChan <- false
			return
		}
		// other goroutines has commited current log
		if logIndex >= 0 && rf.nextIndex[server] > logIndex {
			rf.mu.Unlock()
			appendChan <- true
			return
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			LeaderCommit: rf.commitIndex,
		}
		if args.PrevLogIndex >= 0 {
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		}
		if logIndex >= 0 {
			args.Entries = rf.log[rf.nextIndex[server] : logIndex+1]
		} else {
			// heartbeat with all entries
			args.Entries = rf.log[rf.nextIndex[server]:]
		}

		fmt.Printf("leader %v append log index %v commit %v to server %v PrevLogIndex %v\n", rf.me, logIndex, args.LeaderCommit, server, args.PrevLogIndex)

		// if current append entries fails
		// rf.nextIndex[server] should be less than failIndex
		// however nextIndex should be non-negative
		failIndex := max(rf.nextIndex[server]-1, 0)
		// failIndex := rf.nextIndex[server] - 1
		rf.mu.Unlock()
		reply := AppendEntriesReply{}
		resultChan := make(chan bool)
		go func() {
			resultChan <- rf.sendAppendEntries(server, &args, &reply)
		}()

		select {
		// rpc timeout for 300 ms
		case <-time.After(300 * time.Millisecond):
			fmt.Printf("leader %v append log index %v to server %v timeout\n", rf.me, logIndex, server)
			appendChan <- false
			break loop
		case rst := <-resultChan:
			if !rst {
				fmt.Printf("leader %v append log index %v to server %v connection fail\n", rf.me, logIndex, server)
				appendChan <- false
				break loop
			}
			if reply.Success {
				rf.mu.Lock()
				rf.nextIndex[server] = max(rf.nextIndex[server], logIndex+1)

				fmt.Printf("leader %v append log index %v to server %v success\n", rf.me, logIndex, server)
				rf.mu.Unlock()
				appendChan <- true
				break loop
			}
			rf.mu.Lock()
			rf.nextIndex[server] = min(rf.nextIndex[server], failIndex)
			rf.mu.Unlock()
			// sleep before retry
			time.Sleep(10 * time.Millisecond)
		}
	}
	rf.mu.Lock()
	rf.updateFlag[server] = 0
	rf.mu.Unlock()
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

// start an election
// @returns if the election is successful
func (rf *Raft) requestForLeader() bool {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = -1 // clean vote flag
	rf.mu.Unlock()

	fmt.Printf("node %d start leader election\n", rf.me)
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}

	// LastLogIndex should be commited log index
	args.LastLogIndex = rf.commitIndex
	if args.LastLogIndex >= 0 {
		args.LastLogTerm = rf.log[args.LastLogIndex].Term
	}

	voteCh := make(chan bool)

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(peer int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(peer, &args, &reply)
				fmt.Printf("node %d request vote RPC to %d:%v\n", rf.me, i, ok)
				voteCh <- ok && reply.VoteGranted
			}(i)
		}
	}

	votedCount := 1
loop:
	for {
		select {
		case <-time.After(300 * time.Millisecond):
			break loop
		case msg := <-voteCh:
			if msg {
				votedCount++

				if votedCount<<1 > len(rf.peers) {
					// get majority vote
					break loop
				}
			}
		}
	}

	fmt.Printf("node %d get %d\n", rf.me, votedCount)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	return votedCount<<1 > len(rf.peers) && rf.votedFor == -1
}

// for 3A args can be empty -> just to update lastAppendEntriesTime
func (rf *Raft) broadcastHeartbeat() {
	atomic.AddInt32(&rf.broadcastCount, 1)
	fmt.Printf("leader %v broadcast heartbeat %d\n", rf.me, atomic.LoadInt32(&rf.broadcastCount))
	// broadcast in current routine
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
			rf.mu.Unlock()
			if duration > time.Duration(1000)*time.Millisecond {
				fmt.Printf("node %d duration %v\n", rf.me, duration)
				rst := rf.requestForLeader()
				for !rst {
					rf.mu.Lock()
					if rf.votedFor != -1 {
						// vote for other leader
						rf.mu.Unlock()
						break
					}
					rf.mu.Unlock()
					// pause for a random amount of time (in case of split vote)
					ms = int(rand.Int63() % 300)
					time.Sleep(time.Duration(ms) * time.Millisecond)
					rst = rf.requestForLeader()
				}
				rf.mu.Lock()
				// win the election
				if rf.votedFor == -1 {
					rf.votedFor = rf.me
					for i := 0; i < len(rf.peers); i++ {
						if i == rf.me {
							continue
						}
						// set nextIndex to the current commit index after leader election
						rf.nextIndex[i] = rf.commitIndex + 1
					}
					rf.mu.Unlock()
					// broadcast heartbeat after leader election
					rf.broadcastHeartbeat()
				} else {
					rf.mu.Unlock()
				}
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

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	rf.commitIndex = -1
	rf.lastApplied = -1

	// 3B does not use matchIndex(?)
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 0
	}

	// rf.matchIndex = make([]int, len(peers))

	rf.lastAppendEntriesTime = time.Now()
	rf.broadcastCount = 0

	rf.updateFlag = make([]int, len(peers))
	for i := range rf.updateFlag {
		rf.updateFlag[i] = 0
	}

	// 3B
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
