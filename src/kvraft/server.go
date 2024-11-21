package kvraft

import (
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
func (kv *KVServer) appendDuplicationChecker(clerk int64, request int) bool {
	buff, contains := kv.buffer[clerk]
	return contains && buff.RequestId >= request
}

/**
 * follower need to apply log to state machine
 */

// lock should be held before invoke handler
func (kv *KVServer) AbstractReplication(logIndex int, requestOp Op, handler func()) Err {
	for {
		time.Sleep(10 * time.Millisecond)

		kv.lock(&kv.mu)
		if kv.killed() {
			delete(kv.leaderResponse, logIndex)
			kv.unlock(&kv.mu)
			return ErrKilledServer
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
			handler()
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
	log := Op{Type: GET, Key: args.Key, Value: ""}
	index, _, leader := kv.rf.Start(log)
	if !leader {
		reply.Err = ErrWrongLeader
		kv.unlock(&kv.mu)
		return
	}

	DPrintf("server %v get %v\n", kv.me, formatOp(log))
	kv.leaderResponse[index] = true
	kv.unlock(&kv.mu)

	// wait for agreement
	reply.Err = kv.AbstractReplication(index, log, func() {
		// do nothing
		DPrintf("server %v get key %s value %s\n", kv.me, args.Key, kv.kvs[args.Key])
		reply.Value = kv.kvs[args.Key]
	})
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.lock(&kv.mu)
	log := Op{Type: PUT, Key: args.Key, Value: args.Value}
	index, _, leader := kv.rf.Start(log)
	if !leader {
		reply.Err = ErrWrongLeader
		kv.unlock(&kv.mu)
		return
	}

	DPrintf("server %v put %v\n", kv.me, formatOp(log))
	kv.leaderResponse[index] = true
	kv.unlock(&kv.mu)

	reply.Err = kv.AbstractReplication(index, log, func() {
		kv.kvs[args.Key] = args.Value
		DPrintf("server %v put key %s value %s\n", kv.me, args.Key, args.Value)
	})
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// check buffer first
	kv.lock(&kv.mu)

	log := Op{Type: APPEND, Key: args.Key, Value: args.Value, ClerkId: args.ClerkMeta.ClerkId, RequestId: args.ClerkMeta.RequestId}
	if kv.appendDuplicationChecker(args.ClerkMeta.ClerkId, args.ClerkMeta.RequestId) {
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

	DPrintf("server %v start append %v\n", kv.me, formatOp(log))

	kv.leaderResponse[index] = true
	kv.unlock(&kv.mu)

	reply.Err = kv.AbstractReplication(index, log, func() {
		if kv.appendDuplicationChecker(args.ClerkMeta.ClerkId, args.ClerkMeta.RequestId) {
			DPrintf("server %v get duplicated append %v\n", kv.me, formatOp(log))
			return
		}

		value, contains := kv.kvs[args.Key]
		if !contains {
			kv.kvs[args.Key] = args.Value
		} else {
			kv.kvs[args.Key] = value + args.Value
		}
		DPrintf("server %v append key %s value %s\n", kv.me, args.Key, kv.kvs[args.Key])

		if buff, contains := kv.buffer[args.ClerkMeta.ClerkId]; contains && buff.RequestId+1 < args.ClerkMeta.RequestId {
			DPrintf("server %v buffer %v get a larger append %v \n", kv.me, buff, args.ClerkMeta.RequestId)
		}

		kv.buffer[args.ClerkMeta.ClerkId] = ResponseBuffer{RequestId: args.ClerkMeta.RequestId, Value: kv.kvs[args.Key]}
	})
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
		kv.commitedQueue = append(kv.commitedQueue, msg)
		DPrintf("server %v read %v\n", kv.me, formatMessage(msg))
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

		process_cnt := 0
		for _, msg := range kv.commitedQueue {
			// msg is marked (should be handled by AbstractReplication)
			if kv.leaderResponse[msg.CommandIndex] {
				break
			}
			DPrintf("server %v synchronize %v\n", kv.me, formatMessage(msg))
			operation := msg.Command.(Op)
			switch operation.Type {
			case PUT:
				kv.kvs[operation.Key] = operation.Value
			case APPEND:
				if kv.appendDuplicationChecker(operation.ClerkId, operation.RequestId) {
					DPrintf("server %v get duplicated append %v\n", kv.me, formatOp(operation))
					break
				}

				value, contains := kv.kvs[operation.Key]
				if !contains {
					kv.kvs[operation.Key] = operation.Value
				} else {
					kv.kvs[operation.Key] = value + operation.Value
				}

				if buff, contains := kv.buffer[operation.ClerkId]; contains && buff.RequestId+1 < operation.RequestId {
					DPrintf("server %v buffer %v get a larger append %v \n", kv.me, buff, operation.RequestId)
				}

				kv.buffer[operation.ClerkId] = ResponseBuffer{RequestId: operation.RequestId, Value: kv.kvs[operation.Key]}
			case GET:
				// do nothing
			default:
				DPrintf("unknown operation %v\n", operation)
			}
			process_cnt++
		}

		kv.commitedQueue = kv.commitedQueue[process_cnt:]
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

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvs = make(map[string]string)
	kv.buffer = make(map[int64]ResponseBuffer)

	kv.leaderResponse = make(map[int]bool)

	kv.debug = false

	// incase of blocking apply channel, buffer the log first, then apply
	// read apply channel endlessly
	go kv.stateReader()
	// only follower need applier to update state machine
	go kv.stateApplier()

	return kv
}
