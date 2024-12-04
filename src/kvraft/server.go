package kvraft

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Printf(format, a...)
	}
	return
}

const (
	GET = iota
	PUT
	APPEND
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  int
	Key   string
	Value string
	// used for Append
	ClerkId   int64
	RequestId int
}

type ResponseBuffer struct {
	RequestId int
	Value     string
}

type LeaderLog struct {
	Operation Op
	Err       Err
	Value     string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.

	kvs    map[string]string        // key-value store
	buffer map[int64]ResponseBuffer // clerk id -> response buffer

	leaderLog map[int]LeaderLog
	// record the last log index of state
	stateIndex int

	debug bool
}

func (kv *KVServer) lock(lock *sync.Mutex, sign ...string) {
	lock.Lock()
	if kv.debug {
		_, _, line, _ := runtime.Caller(1)
		if len(sign) > 0 {
			DPrintf("server %d lock %v at %v\n", kv.me, sign[0], line)
		} else {
			DPrintf("server %d lock mu at %v\n", kv.me, line)
		}
	}
}

func (kv *KVServer) unlock(lock *sync.Mutex, sign ...string) {
	lock.Unlock()
	if kv.debug {
		_, _, line, _ := runtime.Caller(1)
		if len(sign) > 0 {
			DPrintf("server %d unlock %v at %v\n", kv.me, sign[0], line)
		} else {
			DPrintf("server %d unlock mu at %v\n", kv.me, line)
		}
	}
}

func formatOp(op Op) string {
	switch op.Type {
	case GET:
		return fmt.Sprintf("GET key %s", op.Key)
	case PUT:
		return fmt.Sprintf("PUT key %s value %s", op.Key, op.Value)
	case APPEND:
		return fmt.Sprintf("APPEND key %s value %s clerk id %v request id %v", op.Key, op.Value, op.ClerkId, op.RequestId)
	default:
		return "UNKNOWN"
	}
}

func formatMessage(message raft.ApplyMsg) string {
	op := message.Command.(Op)
	return fmt.Sprintf("index %v op %v", message.CommandIndex, formatOp(op))
}

// lock should be held before invoke checker
func (kv *KVServer) requestDuplicationChecker(clerk int64, request int) bool {
	buff, contains := kv.buffer[clerk]
	return contains && buff.RequestId >= request
}

// lock should be held before invoke handler
// get handler does not update kv, just update buffer
func (kv *KVServer) getHandler(op Op) {
	if kv.requestDuplicationChecker(op.ClerkId, op.RequestId) {
		DPrintf("server %v get duplicated get %v\n", kv.me, formatOp(op))
		return
	}

	if buff, contains := kv.buffer[op.ClerkId]; contains && buff.RequestId+1 < op.RequestId {
		DPrintf("server %v buffer %v get a larger get %v \n", kv.me, buff, op.RequestId)
	}

	value := kv.kvs[op.Key]
	kv.buffer[op.ClerkId] = ResponseBuffer{RequestId: op.RequestId, Value: value}
	DPrintf("server %v get key %s value %s\n", kv.me, op.Key, value)
}

// lock should be held before invoke handler
func (kv *KVServer) putHandler(op Op) {
	if kv.requestDuplicationChecker(op.ClerkId, op.RequestId) {
		DPrintf("server %v get duplicated put %v\n", kv.me, formatOp(op))
		return
	}

	kv.kvs[op.Key] = op.Value
	if buff, contains := kv.buffer[op.ClerkId]; contains && buff.RequestId+1 < op.RequestId {
		DPrintf("server %v buffer %v get a larger put %v \n", kv.me, buff, op.RequestId)
	}

	kv.buffer[op.ClerkId] = ResponseBuffer{RequestId: op.RequestId, Value: op.Value}
	DPrintf("server %v put key %s value to %s\n", kv.me, op.Key, op.Value)
}

func (kv *KVServer) appendHandler(op Op) {
	if kv.requestDuplicationChecker(op.ClerkId, op.RequestId) {
		DPrintf("server %v get duplicated append %v\n", kv.me, formatOp(op))
		return
	}

	value, contains := kv.kvs[op.Key]
	if !contains {
		kv.kvs[op.Key] = op.Value
	} else {
		kv.kvs[op.Key] = value + op.Value
	}

	if buff, contains := kv.buffer[op.ClerkId]; contains && buff.RequestId+1 < op.RequestId {
		DPrintf("server %v buffer %v get a larger append %v \n", kv.me, buff, op.RequestId)
	}

	kv.buffer[op.ClerkId] = ResponseBuffer{RequestId: op.RequestId, Value: kv.kvs[op.Key]}
	DPrintf("server %v append key %s value to %s\n", kv.me, op.Key, kv.kvs[op.Key])
}

// lock should be held before invoke handler
func (kv *KVServer) snapshotCheck() {
	// -1 means no snapshot
	if kv.maxraftstate == -1 {
		return
	}

	// log size is less than maxraftstate
	if kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}

	snapshot := kv.makeSnapshot()
	kv.rf.Snapshot(kv.stateIndex, snapshot)
	DPrintf("server %v make snapshot at index %v kvs %v buffer %v\n", kv.me, kv.stateIndex, kv.kvs, kv.buffer)
}

func (kv *KVServer) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateIndex)
	e.Encode(kv.kvs)
	e.Encode(kv.buffer)

	return w.Bytes()
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var logIndex int
	var kvs map[string]string
	var buffer map[int64]ResponseBuffer

	if d.Decode(&logIndex) != nil || d.Decode(&kvs) != nil || d.Decode(&buffer) != nil {
		DPrintf("server %v read snapshot failed\n", kv.me)
		return
	}
	kv.stateIndex = logIndex
	kv.kvs = kvs
	kv.buffer = buffer

	DPrintf("server %v read snapshot kvs %v buffer %v\n", kv.me, kv.kvs, kv.buffer)
}

// FakeLogHandler just check response, do not handle log
func (kv *KVServer) FakeLogHandler(logIndex int) (Err, string) {
	defer kv.unlock(&kv.mu)
	defer delete(kv.leaderLog, logIndex)
	for {
		time.Sleep(10 * time.Millisecond)
		kv.lock(&kv.mu)
		if kv.killed() {
			return ErrKilledServer, ""
		}
		if _, leader := kv.rf.GetState(); !leader {
			return ErrWrongLeader, ""
		}
		if leaderLog := kv.leaderLog[logIndex]; leaderLog.Err != ErrPending {
			return leaderLog.Err, leaderLog.Value
		}
		kv.unlock(&kv.mu)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.lock(&kv.mu)
	log := Op{Type: GET, Key: args.Key, ClerkId: args.ClerkMeta.ClerkId, RequestId: args.ClerkMeta.RequestId}
	if kv.requestDuplicationChecker(args.ClerkMeta.ClerkId, args.ClerkMeta.RequestId) {
		reply.Err, reply.Value = OK, kv.buffer[args.ClerkMeta.ClerkId].Value
		kv.unlock(&kv.mu)
		DPrintf("server %v get duplicated get %v\n", kv.me, formatOp(log))
		return
	}

	index, _, leader := kv.rf.Start(log)
	if !leader {
		reply.Err = ErrWrongLeader
		kv.unlock(&kv.mu)
		return
	}

	DPrintf("server %v mark get index %v log %v\n", kv.me, index, formatOp(log))

	kv.leaderLog[index] = LeaderLog{Err: ErrPending, Value: "", Operation: log}
	kv.unlock(&kv.mu)

	// wait for agreement
	reply.Err, reply.Value = kv.FakeLogHandler(index)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.lock(&kv.mu)
	log := Op{Type: PUT, Key: args.Key, Value: args.Value, ClerkId: args.ClerkMeta.ClerkId, RequestId: args.ClerkMeta.RequestId}
	if kv.requestDuplicationChecker(args.ClerkMeta.ClerkId, args.ClerkMeta.RequestId) {
		reply.Err = OK
		kv.unlock(&kv.mu)
		DPrintf("server %v get duplicated put %v\n", kv.me, formatOp(log))
		return
	}

	index, _, leader := kv.rf.Start(log)
	if !leader {
		reply.Err = ErrWrongLeader
		kv.unlock(&kv.mu)
		return
	}

	DPrintf("server %v mark put index %v log %v\n", kv.me, index, formatOp(log))
	kv.leaderLog[index] = LeaderLog{Err: ErrPending, Value: "", Operation: log}
	kv.unlock(&kv.mu)

	// wait for agreement
	reply.Err, _ = kv.FakeLogHandler(index)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// check buffer first
	kv.lock(&kv.mu)

	log := Op{Type: APPEND, Key: args.Key, Value: args.Value, ClerkId: args.ClerkMeta.ClerkId, RequestId: args.ClerkMeta.RequestId}
	if kv.requestDuplicationChecker(args.ClerkMeta.ClerkId, args.ClerkMeta.RequestId) {
		reply.Err = OK
		kv.unlock(&kv.mu)
		DPrintf("server %v get duplicated append %v\n", kv.me, formatOp(log))
		return
	}

	index, _, leader := kv.rf.Start(log)
	if !leader {
		reply.Err = ErrWrongLeader
		kv.unlock(&kv.mu)
		return
	}

	DPrintf("server %v mark append index %v log %v\n", kv.me, index, formatOp(log))
	kv.leaderLog[index] = LeaderLog{Err: ErrPending, Value: "", Operation: log}
	kv.unlock(&kv.mu)

	// wait for agreement
	reply.Err, _ = kv.FakeLogHandler(index)
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

func (kv *KVServer) stateReader() {
	for msg := range kv.applyCh {
		kv.lock(&kv.mu)
		// server read log only
		if msg.CommandValid {
			DPrintf("server %v read %v\n", kv.me, formatMessage(msg))
			kv.commandApplier(msg)
		} else if msg.SnapshotValid {
			// update state by snapshot !!!
			if msg.SnapshotIndex > kv.stateIndex {
				DPrintf("server %v read snapshot jump from %v to %v\n", kv.me, kv.stateIndex, msg.SnapshotIndex)
				kv.readSnapshot(msg.Snapshot)
				kv.stateIndex = msg.SnapshotIndex
			}
		}
		kv.unlock(&kv.mu)
	}
}

// lock should be held before invoke applier
func (kv *KVServer) commandApplier(msg raft.ApplyMsg) {
	operation := msg.Command.(Op)
	flag := false
	// applier only handle unprocessed logs
	if msg.CommandIndex > kv.stateIndex {
		kv.stateIndex = msg.CommandIndex
		flag = true
		DPrintf("server %v process %v\n", kv.me, formatMessage(msg))

		switch operation.Type {
		case PUT:
			kv.putHandler(operation)
		case APPEND:
			kv.appendHandler(operation)
		case GET:
			kv.getHandler(operation)
		default:
			DPrintf("unknown operation %v\n", operation)
		}
		kv.snapshotCheck()
	}

	if leaderLog, contains := kv.leaderLog[msg.CommandIndex]; contains {
		if leaderLog.Operation.Type == operation.Type && leaderLog.Operation.Key == operation.Key && flag {
			leaderLog.Err = OK
			leaderLog.Value = kv.kvs[operation.Key]
		} else {
			leaderLog.Err = ErrNoAgreement
		}
		kv.leaderLog[msg.CommandIndex] = leaderLog
		// incase of memory leak
		DPrintf("server %v process waiting message %v log value %v\n", kv.me, formatMessage(msg), leaderLog.Value)
	}
}

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
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvs = make(map[string]string)
	kv.buffer = make(map[int64]ResponseBuffer)

	// kv.leaderResponse = make(map[int]Response)
	kv.leaderLog = make(map[int]LeaderLog)
	kv.debug = false
	// kv.debug = true

	kv.stateIndex = 0
	kv.readSnapshot(persister.ReadSnapshot())

	// incase of blocking apply channel, buffer the log first, then apply
	// read apply channel endlessly
	go kv.stateReader()

	return kv
}
