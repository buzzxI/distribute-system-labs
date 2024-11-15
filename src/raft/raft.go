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
	"runtime"
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

const (
	Follower = iota
	Candidate
	Leader
)

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
	// voteFor -1 indicates no vote (follower)
	// voteFor -2 indicates vote for self (candidate)
	votedFor int
	log      []LogEntry

	// volatile state for all server
	commitIndex int // index of highest log entry known to be commit
	lastApplied int // index of highest log entry applied to state machine (lastApplied < commitIndex)

	// volatile state for leader
	// these fields are reinitialized after leader election
	nextIndex []int // index of next log entry send to each server, initialized to length of peers
	// matchIndex []int // index of highest log entry known to be replicated on server

	lastAppendEntriesTime time.Time

	// 3B
	applyCh chan ApplyMsg // leader will commit msg to this channel

	// use to count broadcast
	broadcastCount     int
	confirmedBroadcast int
	// if flag[i] = 1 means heartbeat to i has not finished
	boradcastFlag []int

	replicaProcess []int
	role           int
	// debug flag -> print lock/unlock info
	debug bool

	// raft store log from offset (initialized to 0)
	// logOffset         int
	// snapshot fields
	lastIncludedIndex int
	lastIncludedTerm  int
	// commitLock        sync.Mutex

	leaderSnapshotData map[int][]byte // declare a map offset as key, raw data as value
	snapshotDataDone   int            // snapshotDone is -1 if leader snapshot has not finished
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.votedFor == rf.me
}

// func (rf *Raft) getLine() int {
// 	_, _, line, _ := runtime.Caller(1)
// 	return line
// }

func (rf *Raft) lock(lock *sync.Mutex, sign ...string) {
	lock.Lock()
	if rf.debug {
		_, _, line, _ := runtime.Caller(1)
		if len(sign) > 0 {
			fmt.Printf("node %d lock %v at %v\n", rf.me, sign[0], line)
		} else {
			fmt.Printf("node %d lock mu at %v\n", rf.me, line)
		}
	}
}

func (rf *Raft) unlock(lock *sync.Mutex, sign ...string) {
	lock.Unlock()
	if rf.debug {
		_, _, line, _ := runtime.Caller(1)
		if len(sign) > 0 {
			fmt.Printf("node %d unlock %v at %v\n", rf.me, sign[0], line)
		} else {
			fmt.Printf("node %d unlock mu at %v\n", rf.me, line)
		}
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
// goroutine should hold the lock while invoke this function
func (rf *Raft) persist(snapshot []byte) {
	// Your code here (3C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	fmt.Printf("node %v persist current term %v vote for %v last included index %v term %v log %v\n", rf.me, rf.currentTerm, rf.votedFor, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.log)
	// fmt.Printf()
	raftstate := w.Bytes()
	// if snapshot is nil, then current persist is a cascaded persist (raft state only, snapshot should be the same)
	if snapshot == nil {
		snapshot = rf.persister.ReadSnapshot()
	}
	rf.persister.Save(raftstate, snapshot)
}

func (rf *Raft) readRaftState(data []byte) {
	fmt.Printf("node %v read raft state\n", rf.me)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) == nil &&
		d.Decode(&votedFor) == nil &&
		d.Decode(&log) == nil &&
		d.Decode(&lastIncludedIndex) == nil &&
		d.Decode(&lastIncludedTerm) == nil {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		if rf.votedFor == -2 {
			rf.votedFor = -1
			fmt.Printf("node %v read candidate state\n", rf.me)
		}

		rf.log = log
		fmt.Printf("node %v read term %v vote %v log %v\n", rf.me, currentTerm, votedFor, log)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			rf.nextIndex[i] = rf.commitIndex + 1
		}

		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = rf.lastIncludedIndex // initialize commit index to lastInclude index
		rf.lastApplied = rf.lastIncludedIndex
	}
}

func (rf *Raft) readSnapshot(data []byte) {
	fmt.Printf("node %v read snapshot\n", rf.me)
	// apply the message to apply channel (use go routine to not block raft instance)
	go func() {
		rf.applyCh <- ApplyMsg{SnapshotValid: true, Snapshot: data, SnapshotIndex: rf.lastIncludedIndex + 1, SnapshotTerm: rf.lastIncludedTerm}
	}()
}

// restore previously persisted state.
// no need to hold the lock (initial stage)
func (rf *Raft) readPersist(state []byte, snapshot []byte) {
	fmt.Printf("node %v read persist\n", rf.me)

	stateSize := rf.persister.RaftStateSize()

	if stateSize > 0 {
		rf.readRaftState(state)
	}

	// snapshotSize := rf.persister.SnapshotSize()

	// if snapshotSize > 0 {
	// 	rf.readSnapshot(snapshot)
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// raft should discard log entries before index (including index)
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D)

	// judge index start from 1
	index--
	fmt.Printf("node %v get Snapshot %v\n", rf.me, index)

	// use go routine to finsh invocation of snapshot quickly
	// go func(index int, snapshot []byte) {
	// 	rf.lock(&rf.mu)
	// 	defer rf.unlock(&rf.mu)

	// 	// stale index
	// 	if rf.lastIncludedIndex >= index {
	// 		return
	// 	}

	// 	// get index offset in current log
	// 	logOffset := index - rf.lastIncludedIndex - 1

	// 	fmt.Printf("node %v snapshot %v, rf last include index %v term %v\n", rf.me, index, rf.lastIncludedIndex, rf.lastIncludedTerm)

	// 	rf.lastIncludedIndex = index
	// 	rf.lastIncludedTerm = rf.log[logOffset].Term
	// 	rf.log = rf.log[logOffset+1:]

	// 	rf.persist(snapshot)
	// }(index, snapshot)

	func(index int, snapshot []byte) {
		rf.lock(&rf.mu)
		defer rf.unlock(&rf.mu)

		// stale index
		if rf.lastIncludedIndex >= index {
			return
		}

		// get index offset in current log
		logOffset := index - rf.lastIncludedIndex - 1

		fmt.Printf("node %v snapshot %v, rf last include index %v term %v\n", rf.me, index, rf.lastIncludedIndex, rf.lastIncludedTerm)

		rf.lastIncludedIndex = index
		rf.lastIncludedTerm = rf.log[logOffset].Term
		rf.log = rf.log[logOffset+1:]

		rf.persist(snapshot)
	}(index, snapshot)

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

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int    // byte offset where chunk is positioned in the snapshot file
	Data              []byte // raw byte of snapshot
	Done              bool   // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("node %d get request vote from %d term %v current term %v last log index %v last log term %v\n",
		rf.me, args.CandidateId, args.Term, rf.currentTerm, args.LastLogIndex, args.LastLogTerm)

	// candidate's term is less than current term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		// candidate/leader has larger term -> transfer to follower
		rf.currentTerm = args.Term
		if rf.votedFor == rf.me {
			fmt.Printf("leader %d transfer to follower\n", rf.me)
		}
		rf.votedFor = -1
		rf.persist(nil)
	}

	lastIndex, lastTerm := rf.getLastLogIndexTerm()

	// candidate's log should be at least as up-to-date as log for majority of servers
	if args.LastLogTerm < lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex < lastIndex) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.votedFor == -1 {
		// follower (voteable)
		fmt.Printf("node %d grant request vote from %d term %d\n", rf.me, args.CandidateId, args.Term)
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		// added for 3C
		rf.persist(nil)
		// update last append entries time
		rf.lastAppendEntriesTime = time.Now()
	} else {
		// cannot vote (grant vote only if args candidateId is the same as votedFor)
		reply.Term = rf.currentTerm
		reply.VoteGranted = rf.votedFor == args.CandidateId
		if reply.VoteGranted {
			fmt.Printf("node %d grant request vote from %d term %d\n", rf.me, args.CandidateId, args.Term)
		} else {
			fmt.Printf("node %d reject request vote from %d vote for %d\n", rf.me, args.CandidateId, rf.votedFor)
		}
	}
}

// server will reject AppendEntriesRPC, if PrevLogIndex and PrevLogTerm mismatch
// same index, same term -> same command
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock(&rf.mu)
	defer rf.unlock(&rf.mu)
	fmt.Printf("node %d get append entry from %d current term %d arg term %d rf commit %v commit index %v prev index %v logs %v\n",
		rf.me, args.LeaderId, rf.currentTerm, args.Term, rf.commitIndex, args.LeaderCommit, args.PrevLogIndex, args.Entries)

	reply.Index = rf.commitIndex
	// AppendRPC with stale term -> reject
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		// get AppendRPC from larger term -> transfer to follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist(nil)
	}

	// get AppendRPC from current term with different leader -> reject
	if args.Term == rf.currentTerm && rf.votedFor >= 0 && rf.votedFor != args.LeaderId {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	fmt.Printf("node %d get valid append entry from %d\n", rf.me, args.LeaderId)

	// get append entry from another leader (with term at least as large as current term)
	rf.votedFor = args.LeaderId
	// added for 3C
	rf.persist(nil)

	now := time.Now()
	if now.After(rf.lastAppendEntriesTime) {
		rf.lastAppendEntriesTime = now
	}

	// prev log mismatch
	totalLen := len(rf.log) + rf.lastIncludedIndex + 1
	if totalLen <= args.PrevLogIndex {
		fmt.Printf("node %d reject append entry from %d log length %v PrevLogIndex %v\n", rf.me, args.LeaderId, totalLen, args.PrevLogIndex)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 3D may cause such problem
	prevLogIndexOffset := args.PrevLogIndex - rf.lastIncludedIndex - 1
	// current server has already trim the log, loss info about log
	if prevLogIndexOffset < -1 {
		fmt.Printf("node %d get append rpc with trimed prevLogIndex %v\n", rf.me, prevLogIndexOffset)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if (prevLogIndexOffset == -1 && rf.lastIncludedTerm != args.PrevLogTerm) ||
		(prevLogIndexOffset >= 0 && rf.log[prevLogIndexOffset].Term != args.PrevLogTerm) {
		if prevLogIndexOffset >= 0 {
			fmt.Printf("node %d reject append entry from %d log term %v PrevLogTerm %v\n", rf.me, args.LeaderId, rf.log[prevLogIndexOffset].Term, args.PrevLogTerm)
		} else {
			fmt.Printf("node %d reject append entry from %d log term %v PrevLogTerm %v\n", rf.me, args.LeaderId, rf.lastIncludedTerm, args.PrevLogTerm)
		}
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// append log entries
	if len(args.Entries) > 0 {
		i := 0
		j := prevLogIndexOffset + 1
		// same index, term -> same command
		// jump through same command
		for i < len(args.Entries) && j < len(rf.log) {
			if args.Entries[i].Term != rf.log[j].Term {
				break
			}
			i++
			j++
		}
		fmt.Printf("node %d append log from %v args(i) %v self(j) %v\n", rf.me, args.LeaderId, i, j+rf.lastIncludedIndex+1)
		// append remaining log
		if i < len(args.Entries) {
			// in case of data race
			rf.log = rf.log[:j]
			for k := i; k < len(args.Entries); k++ {
				entry := args.Entries[k]
				rf.log = append(rf.log, entry)
			}
			// rf.log = append(rf.log[:j], args.Entries[i:]...)
			// added for 3C
			rf.persist(nil)
		}
		fmt.Printf("node %d append log from %d log %v\n", rf.me, args.LeaderId, rf.log)
	}

	commitIndex := min(args.LeaderCommit, max(rf.lastIncludedIndex+len(rf.log), 0))
	if commitIndex > rf.commitIndex {
		rf.commitIndex = commitIndex
	}

	fmt.Printf("node %v grant append rpc from %v, args commit index %v, rf commit index %v, rf last included index %v term %v, log %v\n", rf.me, args.LeaderId, args.LeaderCommit, rf.commitIndex, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.log)
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lock(&rf.mu)
	defer rf.unlock(&rf.mu)

	// reply immediately if leader get stale term
	if args.Term < rf.currentTerm {
		reply.Term = args.Term
		// rf.unlock(&rf.mu)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist(nil)
	}

	// reply only contains term info...
	reply.Term = rf.currentTerm

	// get AppendRPC from current term with different leader -> reject
	if args.Term == rf.currentTerm && rf.votedFor >= 0 && rf.votedFor != args.LeaderId {
		// rf.unlock(&rf.mu)
		return
	}

	fmt.Printf("node %d get valid install snapshot from %d offset %v last included index %v term %v\n", rf.me, args.LeaderId, args.Offset, args.LastIncludedIndex, args.LastIncludedTerm)

	// store state
	rf.votedFor = args.LeaderId
	rf.persist(nil)

	// update last append time to reset election timer
	now := time.Now()
	if now.After(rf.lastAppendEntriesTime) {
		rf.lastAppendEntriesTime = now
	}

	// consturct a snapshot
	if args.Offset == 0 {
		rf.leaderSnapshotData = make(map[int][]byte)
		rf.leaderSnapshotData[args.Offset] = args.Data
		rf.snapshotDataDone = -1
	}

	if args.Done {
		rf.snapshotDataDone = args.Offset
	}

	flag := true
	snapshot := make([]byte, 0)
	for i := 0; i <= rf.snapshotDataDone; {
		data, ok := rf.leaderSnapshotData[i]
		if !ok {
			flag = false
			break
		}
		snapshot = append(snapshot, data...)
		i += len(data)
	}

	if !flag {
		// snapshot data has not finished
		// rf.unlock(&rf.mu)
		return
	}

	// stale index (if args'last included index is equals to raft'last included index -> stale snapshot)
	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		// rf.unlock(&rf.mu)
		return
	}

	fmt.Printf("node %v install snapshot from %v, rf last include index %v term %v, args last include index %v term %v\n", rf.me, args.LeaderId, rf.lastIncludedIndex, rf.lastIncludedTerm, args.LastIncludedIndex, args.LastIncludedTerm)
	lastLogIndex := rf.lastIncludedIndex + len(rf.log)
	if lastLogIndex < args.LastIncludedIndex {
		rf.log = make([]LogEntry, 0)
	} else {
		// remain some logs ...
		logOffset := args.LastIncludedIndex - rf.lastIncludedIndex - 1
		rf.log = rf.log[logOffset+1:]
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	// trimed log also trim commit index and last applied
	rf.commitIndex = max(args.LastIncludedIndex, rf.commitIndex)
	rf.lastApplied = rf.commitIndex

	rf.persist(snapshot)
	// commit lock is used to order ApplyMsg to apply channel
	// rf.lock(&rf.commitLock)
	// rf.unlock(&rf.mu)

	// only until the lock is released, msg can be sent to channel
	// used to inform upper layer snapshot change
	// judge index start from 1
	rf.applyCh <- ApplyMsg{SnapshotValid: true, Snapshot: snapshot, SnapshotIndex: args.LastIncludedIndex + 1, SnapshotTerm: args.LastIncludedTerm}
	// rf.unlock(&rf.commitLock)
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// lock should be held by caller
func (rf *Raft) getLastLogIndexTerm() (int, int) {
	if len(rf.log) == 0 {
		// log be trimmed -> last index, term is lastIncluded index, term
		return rf.lastIncludedIndex, rf.lastIncludedTerm
	}
	// return len(rf.log) - 1, rf.log[len(rf.log)-1].Term
	// added for 3D
	return len(rf.log) + rf.lastIncludedIndex, rf.log[len(rf.log)-1].Term
}

// locks should be held by caller
func (rf *Raft) lostLeadership() {
	if rf.votedFor != rf.me {
		// already not a leader
		return
	}

	rf.votedFor = -1 // back to follower

	// reset heartbeat state
	rf.broadcastCount = 0
	rf.confirmedBroadcast = 0
	for i := range rf.boradcastFlag {
		rf.boradcastFlag[i] = 0
	}
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
		rf.persist(nil)
		// added for 3D
		index = len(rf.log) + rf.lastIncludedIndex
		rf.nextIndex[rf.me] = max(rf.nextIndex[rf.me], index+1)
		fmt.Printf("leader %v append entry %v command %v\n", rf.me, index, command)
		go rf.AppendEntriesRPC(index)
	}

	return index + 1, term, isLeader
}

// this function should be invoked by leader only
// append log entry at @param: logIndex to all server
// logIndex can be -1 to indicate current AppendEntriesRPC is a headerbeat
func (rf *Raft) AppendEntriesRPC(logIndex int) bool {
	rf.lock(&rf.mu)
	if rf.votedFor != rf.me {
		rf.unlock(&rf.mu)
		return false
	}
	rf.unlock(&rf.mu)

	rstChan := make(chan bool, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func() {
				if logIndex == -1 {
					rf.lock(&rf.mu)
					if rf.boradcastFlag[i] == 1 {
						rf.unlock(&rf.mu)
						return
					}
					rf.boradcastFlag[i] = 1
					rf.unlock(&rf.mu)
				}
				rstChan <- rf.AppendEntriesRPCToServer(i, logIndex)
				if logIndex == -1 {
					rf.lock(&rf.mu)
					rf.boradcastFlag[i] = 0
					rf.unlock(&rf.mu)
				}
			}()
		}
	}

	appendFailCount := 0
	count := 0
	for msg := range rstChan {
		if !msg {
			appendFailCount++
			if appendFailCount<<1 > len(rf.peers) {
				break
			}
		}
		count++
		if count == len(rf.peers)-1 {
			break
		}
	}

	return appendFailCount<<1 <= len(rf.peers)
}

func (rf *Raft) AppendEntriesRPCToServer(server int, logIndex int) bool {
	retry := 0
	for {
		// retry until the server has been killed
		if rf.killed() {
			return false
		}
		retry++
		rf.lock(&rf.mu)
		// leader changed
		if rf.votedFor != rf.me {
			rf.unlock(&rf.mu)
			return false
		}

		if logIndex >= 0 {
			// other goroutine handles current log replication
			if rf.replicaProcess[server] > logIndex {
				rf.unlock(&rf.mu)
				// true to append channel does not means append success
				return false
			}
			rf.replicaProcess[server] = logIndex
		} else {
			// heartbeat means replica all entries to server
			rf.replicaProcess[server] = rf.lastIncludedIndex + len(rf.log)
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			LeaderCommit: rf.commitIndex,
		}

		prevLogIndexOffset := args.PrevLogIndex - rf.lastIncludedIndex - 1

		if prevLogIndexOffset < -1 {
			fmt.Printf("leader %v invoke InstallSnapshotRPCToServer to %v prevLogIndexOffset %v logIndex %v\n", rf.me, server, prevLogIndexOffset, logIndex)

			/* server lag behind, invoke InstallSnapshot, not AppendRPC */
			rf.unlock(&rf.mu)
			// server will try to install snapshot endlessly until the server has been killed or reponse is gotten
			rst := rf.InstallSnapshotRPCToServer(server)
			if !rst {
				// current server is not leader any more
				return false
			}
			// as lock has been released, back to start, rejudge state
			continue
		} else if prevLogIndexOffset == -1 {
			args.PrevLogTerm = rf.lastIncludedTerm
		} else {
			args.PrevLogTerm = rf.log[prevLogIndexOffset].Term
		}

		// slice end (log end for heartbeat, log index for AppendLogEntries)
		end := rf.replicaProcess[server]

		// make a copy to avoid data race
		len := end - rf.nextIndex[server] + 1
		logCpy := make([]LogEntry, len)

		// added for 3D
		nextIndexOffset := rf.nextIndex[server] - rf.lastIncludedIndex - 1
		endIndexOffset := end - rf.lastIncludedIndex - 1
		copy(logCpy, rf.log[nextIndexOffset:endIndexOffset+1])
		args.Entries = logCpy
		reply := AppendEntriesReply{}

		rf.unlock(&rf.mu)
		resultChan := make(chan bool)
		go func() {
			fmt.Printf("leader %v append log to server %v prev index %v entries %v, retry %v\n",
				rf.me, server, args.PrevLogIndex, args.Entries, retry)
			resultChan <- rf.sendAppendEntries(server, &args, &reply)
		}()

		// timeout or connection fail triggers retry (not just return!)
		select {
		case <-time.After(300 * time.Millisecond):
			fmt.Printf("leader %v append log index %v to server %v timeout, retry %v\n", rf.me, logIndex, server, retry)
		case rst := <-resultChan:
			if !rst {
				fmt.Printf("leader %v append log index %v to server %v connection fail, retry %v\n", rf.me, logIndex, server, retry)
				// connection failure triggers retry
				break
			}
			rf.lock(&rf.mu)
			// lost leadership
			if rf.votedFor != rf.me {
				rf.unlock(&rf.mu)
				return false
			}
			if reply.Success {
				rf.nextIndex[server] = max(rf.nextIndex[server], end+1)
				fmt.Printf("leader %v append log index %v to server %v success, retry %v\n", rf.me, logIndex, server, retry)
				rf.unlock(&rf.mu)
				rf.CheckLogCommitment(end)
				return true
			} else {
				if reply.Term > rf.currentTerm {
					fmt.Printf("leader %v append log index %v to server %v fail, stale term %v reply term %v, lost leadership, retry %v\n", rf.me, logIndex, server, rf.currentTerm, reply.Term, retry)
					rf.currentTerm = reply.Term
					rf.lostLeadership()
					// added for 3C
					rf.persist(nil)
					rf.lastAppendEntriesTime = time.Now()
					rf.unlock(&rf.mu)
					return false
				}

				fmt.Printf("leader %v append log index %v to server %v fail, next index fallback to %v, retry %v\n", rf.me, logIndex, server, reply.Index+1, retry)
				rf.nextIndex[server] = min(rf.nextIndex[server], reply.Index+1)
				rf.unlock(&rf.mu)
			}
		}
		// sleep a while before retry
		// time.Sleep(5 * time.Millisecond)
	}
}

// leader send current snapshot to server
func (rf *Raft) InstallSnapshotRPCToServer(server int) bool {
	retry := 0
	for {
		retry++
		// retry until the server has been killed
		if rf.killed() {
			rf.unlock(&rf.mu)
			return false
		}

		rf.lock(&rf.mu)
		// not a leader any more
		if rf.votedFor != rf.me {
			rf.unlock(&rf.mu)
			return false
		}

		// InstallSnapshotRPCToServer is needed only if offset of nextIndex[server] is less than -1
		logOffset := rf.nextIndex[server] - 1 - rf.lastIncludedIndex - 1
		if logOffset >= -1 {
			rf.unlock(&rf.mu)
			return true
		}

		// this process can be optimized by partition snapshot
		// rather than send the complete snapshot at once, send the snapshot by slice
		snapshot := rf.persister.ReadSnapshot()

		args := InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Offset:            0,
			Data:              snapshot,
			Done:              true,
		}

		rf.unlock(&rf.mu)

		reply := InstallSnapshotReply{}

		rstChan := make(chan bool)

		go func() {
			fmt.Printf("leader %v install snapshot to server %v last included index %v term %v\n", rf.me, server, args.LastIncludedIndex, args.LastIncludedTerm)
			rstChan <- rf.sendInstallSnapshot(server, &args, &reply)
		}()

		select {
		case <-time.After(300 * time.Millisecond):
			fmt.Printf("leader %v install snapshot to server %v timeout, retry %v\n", rf.me, server, retry)
		case msg := <-rstChan:
			if !msg {
				fmt.Printf("leader %v install snapshot to server %v connection fail, retry %v\n", rf.me, server, retry)
				// connection failure triggers retry
				break
			}

			rf.lock(&rf.mu)
			if rf.votedFor != rf.me {
				rf.unlock(&rf.mu)
				return false
			}

			// current server is outdated
			if reply.Term > rf.currentTerm {
				fmt.Printf("leader %v install snapshot to server %v stale term %v reply term %v, lost leadership, retry %v\n", rf.me, server, rf.currentTerm, reply.Term, retry)
				rf.currentTerm = reply.Term
				rf.lostLeadership()
				// added for 3C
				rf.persist(nil)
				rf.lastAppendEntriesTime = time.Now()
				rf.unlock(&rf.mu)
				return false
			}

			fmt.Printf("leader %v install snapshot to server %v success, retry %v\n", rf.me, server, retry)
			// if snapshot has been installed successfully, nextIndex should be lastIncludeIndex + 1
			rf.nextIndex[server] = args.LastIncludedIndex + 1
			rf.unlock(&rf.mu)
			return true
		}
		// sleep a while before retry
		// time.Sleep(5 * time.Millisecond)
	}
}

// judge will block the applyCh, until snapshot finish
// thus, raft need to release the lock, before commit entry to applyCh
func (rf *Raft) CommitEntries(commitedLog []ApplyMsg) {
	for _, msg := range commitedLog {
		fmt.Printf("node %v commit index %v log %v\n", rf.me, msg.CommandIndex-1, msg.Command)
		rf.applyCh <- msg
	}
}

// check if current log can be commited
func (rf *Raft) CheckLogCommitment(logIndex int) {
	rf.lock(&rf.mu)
	defer rf.unlock(&rf.mu)
	fmt.Printf("node %v try to commit %v raft commit index %v\n", rf.me, logIndex, rf.commitIndex)

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

	logOffset := i - rf.lastIncludedIndex - 1
	// raft does not commit previous log (multiple success heartbeat should commit previous log)
	if logOffset >= 0 && rf.log[logOffset].Term < rf.currentTerm && rf.confirmedBroadcast == 0 {
		return
	}

	// commit until i
	rf.commitIndex = i
}

func (rf *Raft) commitChecker() {
	for {
		rf.lock(&rf.mu)
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			fmt.Printf("node %v commit %v\n", rf.me, rf.lastApplied)
			logOffset := rf.lastApplied - rf.lastIncludedIndex - 1
			msg := ApplyMsg{CommandValid: true, Command: rf.log[logOffset].Command, CommandIndex: rf.lastApplied + 1}
			rf.unlock(&rf.mu)
			rf.applyCh <- msg
		} else {
			rf.unlock(&rf.mu)
		}
		time.Sleep(10 * time.Millisecond)
	}
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

func (rf *Raft) RequestVoteRPCToServer(server int, args RequestVoteArgs, voteChan chan bool, rejectChan chan bool) {
	rf.lock(&rf.mu)
	if args.Term != rf.currentTerm || rf.votedFor != -2 {
		rf.unlock(&rf.mu)
		rejectChan <- true
		return
	}

	reply := RequestVoteReply{}
	rf.unlock(&rf.mu)

	rstChan := make(chan bool)
	fmt.Printf("node %d request vote RPC to %d arg term %v last index %v last term %v\n", rf.me, server, args.Term, args.LastLogIndex, args.LastLogTerm)
	go func() {
		rstChan <- rf.sendRequestVote(server, &args, &reply)
	}()

	select {
	case <-time.After(300 * time.Millisecond):
		fmt.Printf("node %d request vote RPC to %d timeout\n", rf.me, server)
		voteChan <- false
	case rst := <-rstChan:
		if !rst {
			fmt.Printf("node %d request vote RPC to %d connection fail\n", rf.me, server)
			voteChan <- false
			return
		}

		rf.lock(&rf.mu)
		// already a leader
		if rf.votedFor == rf.me {
			rf.unlock(&rf.mu)
			voteChan <- true // useless
			return
		}

		// no longer a candidate
		if args.Term != rf.currentTerm || rf.votedFor != -2 {
			rf.unlock(&rf.mu)
			fmt.Printf("node %d get request vote RPC from %d no longer a candidate\n", rf.me, server)
			rejectChan <- true
			return
		}

		if reply.VoteGranted {
			fmt.Printf("node %d get vote from %d\n", rf.me, server)
			voteChan <- true
		} else {
			fmt.Printf("node %d get reject vote from %d\n", rf.me, server)
			voteChan <- false
			if reply.Term > args.Term {
				rf.currentTerm = max(rf.currentTerm, reply.Term)
				rf.votedFor = -1
				// added for 3C
				rf.persist(nil)
				rejectChan <- true
				fmt.Printf("node %d get larger term vote from %d\n", rf.me, server)
			}
		}
		rf.unlock(&rf.mu)
	}
}

// start an election
func (rf *Raft) RequestVoteRPC(args RequestVoteArgs) bool {
	rf.lock(&rf.mu)
	// no longer a candidate
	if rf.votedFor != -2 {
		rf.unlock(&rf.mu)
		return false
	}
	rf.unlock(&rf.mu)

	// use bufferd channel to avoid blocking
	voteCh := make(chan bool, len(rf.peers)-1)
	rejectChan := make(chan bool, len(rf.peers)-1)

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.RequestVoteRPCToServer(i, args, voteCh, rejectChan)
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

	fmt.Printf("candidate %v get vote %v\n", rf.me, voteCount)
	rf.lock(&rf.mu)
	rst := rf.votedFor == -2 && rf.currentTerm == args.Term && (voteCount+1)<<1 > len(rf.peers)
	fmt.Printf("candidate %v voteFor %v current term %v arg term %v\n", rf.me, rf.votedFor, rf.currentTerm, args.Term)
	if rst {
		rf.votedFor = rf.me
		// added for 3C
		rf.persist(nil)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			// set nextIndex to the current commit index after leader election
			rf.nextIndex[i] = rf.commitIndex + 1
		}
		fmt.Printf("node %d become leader\n", rf.me)
		// reset broadcast count
		rf.broadcastCount = 0
		rf.confirmedBroadcast = 0
		rf.unlock(&rf.mu)
		// broadcast heartbeat after leader election
		rf.broadcastHeartbeat()
	} else {
		rf.unlock(&rf.mu)
	}
	return rst
}

// for 3A args can be empty -> just to update lastAppendEntriesTime
func (rf *Raft) broadcastHeartbeat() {
	rf.lock(&rf.mu)
	if rf.votedFor != rf.me {
		rf.unlock(&rf.mu)
		return
	}
	rf.broadcastCount++
	fmt.Printf("leader %v term %v broadcast heartbeat %d\n", rf.me, rf.currentTerm, rf.broadcastCount)
	rf.unlock(&rf.mu)
	// broadcast in current routine
	go func() {
		rst := rf.AppendEntriesRPC(-1)
		rf.lock(&rf.mu)
		if rst {
			rf.confirmedBroadcast++
		}
		rf.unlock(&rf.mu)
	}()
}

func (rf *Raft) attemptForElection() {
	rf.lock(&rf.mu)
	currentTime := time.Now()
	duration := currentTime.Sub(rf.lastAppendEntriesTime)
	if duration > time.Duration(1000)*time.Millisecond {
		fmt.Printf("node %d wait for %v\n", rf.me, duration)
		// process should release lock after break the loop
		// hold lock within the loop (except for rpc)
		retry := 0
		for {
			// retry until killed
			if rf.killed() {
				rf.unlock(&rf.mu)
				return
			}
			retry++
			rf.currentTerm++
			rf.votedFor = -2 // transfer to candidate
			// added for 3C
			rf.persist(nil)
			lastIndex, lastTerm := rf.getLastLogIndexTerm()
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}
			fmt.Printf("node %d start leader election retry: %v\n", rf.me, retry)
			rf.unlock(&rf.mu)
			rst := rf.RequestVoteRPC(args)
			if rst {
				break
			}
			// pause for a random amount of time (in case of split vote)
			ms := int(rand.Int63() % 300)
			time.Sleep(time.Duration(ms) * time.Millisecond)
			rf.lock(&rf.mu)
			// no longer a candidate
			if rf.votedFor != -2 {
				rf.unlock(&rf.mu)
				break
			}
		}
	} else {
		rf.unlock(&rf.mu)
	}
}

// what if current leader read voteFor is -2 ?

// ticker is used for check leader heartbeat (fellower) and broadcast heartbeat (leader)
func (rf *Raft) ticker() {
	// Your code here (3A)
	for !rf.killed() {
		// lab requires no more than 10 times heartbeat per second
		// pause for a random amount of time between 300 and 450 ms
		ms := 300 + int(rand.Int63()%150)
		// milliseconds.
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.lock(&rf.mu)
		if rf.votedFor == rf.me {
			// leader
			rf.unlock(&rf.mu)
			rf.broadcastHeartbeat()
		} else if rf.votedFor == -1 || rf.votedFor >= 0 {
			// follower
			rf.unlock(&rf.mu)
			rf.attemptForElection()
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

// after server restart, judge invoke Make to create a new Raft instance
// old Raft should be killed ...
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// node is initailized as follower
	rf.role = Follower

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
	rf.confirmedBroadcast = 0
	rf.boradcastFlag = make([]int, len(peers))

	for i := range rf.boradcastFlag {
		rf.boradcastFlag[i] = 0
	}

	rf.replicaProcess = make([]int, len(peers))
	for i := range rf.replicaProcess {
		rf.replicaProcess[i] = -1
	}

	// 3B
	rf.applyCh = applyCh

	rf.debug = false
	// rf.debug = true
	rf.lastIncludedIndex = -1
	rf.lastIncludedTerm = -1

	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())
	rf.snapshotDataDone = -1

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.commitChecker()

	return rf
}
