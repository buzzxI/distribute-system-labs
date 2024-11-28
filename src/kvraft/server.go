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

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	commitedQueue []raft.ApplyMsg

	kvs    map[string]string        // key-value store
	buffer map[int64]ResponseBuffer // clerk id -> response buffer

	leaderResponse map[int]bool

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
	DPrintf("server %v put key %s value %s\n", kv.me, op.Key, op.Value)
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
}

// lock should be held before invoke handler
func (kv *KVServer) snapshotCheck(logIndex int) {
	// -1 means no snapshot
	if kv.maxraftstate == -1 {
		return
	}

	// log size is less than maxraftstate
	if kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}

	snapshot := kv.makeSnapshot()
	kv.rf.Snapshot(logIndex, snapshot)
	DPrintf("server %v make snapshot at index %v commitedQueue %v kvs %v buffer %v leaderResponse %v\n", kv.me, logIndex, kv.commitedQueue, kv.kvs, kv.buffer, kv.leaderResponse)
}

func (kv *KVServer) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.commitedQueue)
	e.Encode(kv.kvs)
	e.Encode(kv.buffer)
	e.Encode(kv.leaderResponse)

	return w.Bytes()
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var commitedQueue []raft.ApplyMsg
	var kvs map[string]string
	var buffer map[int64]ResponseBuffer
	var leaderResponse map[int]bool

	if d.Decode(&commitedQueue) != nil || d.Decode(&kvs) != nil || d.Decode(&buffer) != nil || d.Decode(&leaderResponse) != nil {
		DPrintf("server %v read snapshot failed\n", kv.me)
		return
	}

	kv.commitedQueue = commitedQueue
	kv.kvs = kvs
	kv.buffer = buffer
	kv.leaderResponse = leaderResponse

	DPrintf("server %v read snapshot commitedQueue %v kvs %v buffer %v leaderResponse %v\n", kv.me, kv.commitedQueue, kv.kvs, kv.buffer, kv.leaderResponse)
}

/**
 * follower need to apply log to state machine
 */

// lock should be held before invoke handler
func (kv *KVServer) AbstractReplication(logIndex int, requestOp Op, handler func(op Op)) Err {
	for {
		time.Sleep(10 * time.Millisecond)

		kv.lock(&kv.mu)
		if kv.killed() {
			delete(kv.leaderResponse, logIndex)
			kv.unlock(&kv.mu)
			return ErrKilledServer
		}

		if _, leader := kv.rf.GetState(); !leader {
			// lost leader ship
			delete(kv.leaderResponse, logIndex)
			kv.unlock(&kv.mu)
			return ErrWrongLeader
		}

		if len(kv.commitedQueue) == 0 {
			kv.unlock(&kv.mu)
			continue
		}

		msg := kv.commitedQueue[0]
		// generally should be command index < index only
		// if command index > index -> something must be wrong
		if msg.CommandIndex != logIndex {
			kv.unlock(&kv.mu)
			continue
		}

		DPrintf("server %v process %v\n", kv.me, formatMessage(msg))

		delete(kv.leaderResponse, logIndex)
		op := msg.Command.(Op)
		if op.Type == requestOp.Type && op.Key == requestOp.Key && op.Value == requestOp.Value {
			// remove head msg
			kv.commitedQueue = kv.commitedQueue[1:]
			handler(op)
			kv.snapshotCheck(msg.CommandIndex)
			kv.unlock(&kv.mu)
			return OK
		} else {
			kv.unlock(&kv.mu)
			return ErrNoAgreement
		}
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

	DPrintf("server %v get index %v log %v\n", kv.me, index, formatOp(log))
	kv.leaderResponse[index] = true
	kv.unlock(&kv.mu)

	// wait for agreement
	reply.Err = kv.AbstractReplication(index, log, func(log Op) {
		if kv.requestDuplicationChecker(args.ClerkMeta.ClerkId, args.ClerkMeta.RequestId) {
			reply.Value = kv.buffer[args.ClerkMeta.ClerkId].Value
			DPrintf("server %v get duplicated get %v\n", kv.me, formatOp(log))
		} else {
			reply.Value = kv.kvs[args.Key]
			kv.buffer[args.ClerkMeta.ClerkId] = ResponseBuffer{RequestId: args.ClerkMeta.RequestId, Value: reply.Value}
			DPrintf("server %v get key %s value %s\n", kv.me, args.Key, reply.Value)
		}
	})
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

	DPrintf("server %v put index %v log %v\n", kv.me, index, formatOp(log))
	kv.leaderResponse[index] = true
	kv.unlock(&kv.mu)

	reply.Err = kv.AbstractReplication(index, log, kv.putHandler)
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

	DPrintf("server %v append index %v log %v\n", kv.me, index, formatOp(log))

	kv.leaderResponse[index] = true
	kv.unlock(&kv.mu)

	reply.Err = kv.AbstractReplication(index, log, kv.appendHandler)
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
			kv.commitedQueue = append(kv.commitedQueue, msg)
			DPrintf("server %v read %v\n", kv.me, formatMessage(msg))
		} else if msg.SnapshotValid {
			// update state by snapshot !!!
		}
		kv.unlock(&kv.mu)
	}
}

func (kv *KVServer) stateApplier() {
	for {
		time.Sleep(5 * time.Millisecond)
		// terminate go routine, if get killed
		if kv.killed() {
			return
		}
		kv.lock(&kv.mu)

		for len(kv.commitedQueue) > 0 {
			msg := kv.commitedQueue[0]
			// msg is marked (should be handled by AbstractReplication)
			if kv.leaderResponse[msg.CommandIndex] {
				break
			}

			DPrintf("server %v synchronize %v\n", kv.me, formatMessage(msg))

			operation := msg.Command.(Op)
			switch operation.Type {
			case PUT:
				kv.putHandler(operation)
			case APPEND:
				kv.appendHandler(operation)
			case GET:
				// do nothing
			default:
				DPrintf("unknown operation %v\n", operation)
			}
			kv.commitedQueue = kv.commitedQueue[1:]
			kv.snapshotCheck(msg.CommandIndex)
		}

		kv.unlock(&kv.mu)
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

	kv.leaderResponse = make(map[int]bool)
	kv.debug = false
	// kv.debug = true

	kv.readSnapshot(persister.ReadSnapshot())
	// incase of blocking apply channel, buffer the log first, then apply
	// read apply channel endlessly
	go kv.stateReader()
	// only follower need applier to update state machine
	go kv.stateApplier()

	return kv
}
