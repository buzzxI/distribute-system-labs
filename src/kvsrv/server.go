package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ResponseBuffer struct {
	RequestId bool
	Value     string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvs  map[string]string        // key-value store
	buff map[int64]ResponseBuffer // clerk id -> response buffer
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, contains := kv.kvs[args.Key]
	// kv.mu.Unlock()
	if !contains {
		reply.Value = ""
	} else {
		reply.Value = value
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.kvs[args.Key] = args.Value
	reply.Value = args.Value
}

// Only Append cannot be duplicated
func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()

	buff, contains := kv.buff[args.ClerkMeta.ClerkId]
	if contains && buff.RequestId == args.ClerkMeta.RequestId {
		reply.Value = buff.Value
		return
	}

	value, contains := kv.kvs[args.Key]
	if !contains {
		reply.Value = ""
		kv.kvs[args.Key] = args.Value
	} else {
		reply.Value = value
		kv.kvs[args.Key] = value + args.Value
	}
	// kv.mu.Unlock()
	kv.buff[args.ClerkMeta.ClerkId] = ResponseBuffer{RequestId: args.ClerkMeta.RequestId, Value: reply.Value}
}

func StartKVServer() *KVServer {
	// You may need initialization code here.
	return &KVServer{mu: sync.Mutex{}, kvs: make(map[string]string), buff: make(map[int64]ResponseBuffer)}
}
