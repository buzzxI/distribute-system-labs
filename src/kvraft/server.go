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
	RequestId bool
}

type ResponseBuffer struct {
	RequestId bool
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
	buffer map[int64]ResponseBuffer // clerk id -> response buffer'

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

/**
 * follower need to apply log to state machine
 */

// lock should be held before invoke handler
func (kv *KVServer) AbstractReplication(logIndex int, requestOp Op, handler func()) Err {
	fmt.Printf("server %v abstract replication %v logIndex %v\n", kv.me, requestOp, logIndex)
	for {
		time.Sleep(10 * time.Millisecond)
		if kv.killed() {
			return ErrKilledServer
		}

		_, isLeader := kv.rf.GetState()

		if !isLeader {
			return ErrWrongLeader
		}

		kv.lock(&kv.mu)
		if len(kv.commitedQueue) > 0 {
			msg := kv.commitedQueue[0]
			fmt.Printf("server %v get commited %v logIndex %v\n", kv.me, msg, logIndex)
			if msg.CommandIndex < logIndex {
				kv.unlock(&kv.mu)
				continue
			}

			if msg.CommandIndex > logIndex {
				kv.unlock(&kv.mu)
				return ErrNoAgreement
			}

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
		kv.unlock(&kv.mu)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	log := Op{Type: GET, Key: args.Key, Value: ""}
	DPrintf("server %v get %v\n", kv.me, log)
	index, _, leader := kv.rf.Start(log)
	if !leader {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = kv.AbstractReplication(index, log, func() {
		reply.Value = kv.kvs[args.Key]
		DPrintf("server %v get key %s value %s\n", kv.me, args.Key, reply.Value)
	})
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	log := Op{Type: PUT, Key: args.Key, Value: args.Value}
	DPrintf("server %v put %v\n", kv.me, log)
	index, _, leader := kv.rf.Start(log)
	if !leader {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = kv.AbstractReplication(index, log, func() {
		kv.kvs[args.Key] = args.Value
		DPrintf("server %v put key %s value %s\n", kv.me, args.Key, args.Value)
	})
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// check buffer first
	kv.lock(&kv.mu)
	buff, contains := kv.buffer[args.ClerkMeta.ClerkId]
	if contains && buff.RequestId == args.ClerkMeta.RequestId {
		reply.Err = OK
		kv.unlock(&kv.mu)
		DPrintf("server %v get replicated append %v\n", kv.me, args)
		return
	}
	kv.unlock(&kv.mu)

	log := Op{Type: APPEND, Key: args.Key, Value: args.Value}
	DPrintf("server %v append %v\n", kv.me, log)
	index, _, leader := kv.rf.Start(log)
	if !leader {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = kv.AbstractReplication(index, log, func() {
		value, contains := kv.kvs[args.Key]
		if !contains {
			kv.kvs[args.Key] = args.Value
		} else {
			kv.kvs[args.Key] = value + args.Value
		}
		DPrintf("server %v append key %s value %s\n", kv.me, args.Key, kv.kvs[args.Key])
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
		DPrintf("server %v read %v\n", kv.me, msg)
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
		_, isLeader := kv.rf.GetState()
		if isLeader {
			kv.unlock(&kv.mu)
			continue
		}
		if len(kv.commitedQueue) > 0 {
			operation := kv.commitedQueue[0].Command.(Op)
			fmt.Printf("server %v handle %v as follower\n", kv.me, operation)
			switch operation.Type {
			case PUT:
				kv.kvs[operation.Key] = operation.Value
			case APPEND:
				value, contains := kv.kvs[operation.Key]
				if !contains {
					kv.kvs[operation.Key] = operation.Value
				} else {
					kv.kvs[operation.Key] = value + operation.Value
				}
				kv.buffer[operation.ClerkId] = ResponseBuffer{RequestId: operation.RequestId, Value: kv.kvs[operation.Key]}
			case GET:
				// do nothing
			default:
				DPrintf("unknown operation %v\n", operation)
			}
			kv.commitedQueue = kv.commitedQueue[1:]
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

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvs = make(map[string]string)
	kv.buffer = make(map[int64]ResponseBuffer)

	kv.debug = false

	// incase of blocking apply channel, buffer the log first, then apply
	// read apply channel endlessly
	go kv.stateReader()
	// only follower need applier to update state machine
	go kv.stateApplier()

	return kv
}
