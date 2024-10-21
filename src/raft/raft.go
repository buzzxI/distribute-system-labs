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

	lastAppendEntriesTime time.Time

	// use to count broadcast
	broadcastCount int32
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

// used for heartbeats for 3A
// can be empty for 3A
type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	in := time.Now()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("node %d get request vote from %d\n", rf.me, args.CandidateId)

	if args.Term < rf.currentTerm {
		reply.Term = args.Term
		reply.VoteGranted = false
	} else {
		if args.Term > rf.currentTerm || rf.votedFor == args.CandidateId {
			fmt.Printf("node %d grant request vote from %d term %d\n", rf.me, args.CandidateId, args.Term)
			reply.Term = args.Term
			reply.VoteGranted = true
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}
	}
	out := time.Now()
	duration := out.Sub(in)
	fmt.Printf("node %d request vote duration %d\n", rf.me, duration)
}

func (rf *Raft) InnerAppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("node %d get append entry from %d current term %d arg term %d\n", rf.me, args.LeaderId, rf.currentTerm, args.Term)

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term == rf.currentTerm && rf.votedFor != args.LeaderId {
		return
	}

	fmt.Printf("node %d grant append entry from %d\n", rf.me, args.LeaderId)

	// get append entry from another leader
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.lastAppendEntriesTime = time.Now()
}

// for 3A this function just update lastAppendEntriesTime => to avoid leader election
func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	// Your code here (3A).
	in := time.Now()
	rf.InnerAppendEntries(args, reply)
	out := time.Now()
	duration := out.Sub(in)
	fmt.Printf("node %d append entry duration %d\n", rf.me, duration)
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
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
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
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
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
	fmt.Printf("node %v broadcast heartbeat %d\n", rf.me, atomic.LoadInt32(&rf.broadcastCount))

	args := AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: len(rf.log) - 1,
		PrevLogTerm:  rf.log[len(rf.log)-1].Term,
		Entries:      rf.log,
		LeaderCommit: 0, // TODO: implement
	}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(peer int) {
				reply := AppendEntryReply{}
				ok := rf.peers[peer].Call("Raft.AppendEntries", &args, &reply)
				fmt.Printf("node %d(%d) append entry RPC to %d:%v\n", rf.me, atomic.LoadInt32(&rf.broadcastCount), i, ok)
			}(i)
		}
	}
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
	rf.log = make([]LogEntry, 1)
	rf.log[0].Term = 0 // dummy log entry
	rf.lastAppendEntriesTime = time.Now()
	rf.broadcastCount = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
