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
	RequestId int64
	Value     string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvs  map[string]string        // key-value store
	buff map[int64]ResponseBuffer // clerk id -> response buffer
}

// check if the request is duplicated
// if the request is duplicated, fill the oldValue and return true,
// otherwise return false (oldValue will be dummy value)
func (kv *KVServer) CheckDuplicateRequest(ClerkInfo MetaInfo, oldValue *string) bool {
	buff, contains := kv.buff[ClerkInfo.ClerkId]
	if contains && buff.RequestId == ClerkInfo.RequestId {
		*oldValue = buff.Value
		return true
	}
	return false
}

func (kv *KVServer) UpdateClerkState(ClerkInfo MetaInfo, value string) {
	kv.buff[ClerkInfo.ClerkId] = ResponseBuffer{RequestId: ClerkInfo.RequestId, Value: value}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// if kv.CheckDuplicateRequest(args.ClerkMeta, &reply.Value) {
	// 	return
	// }

	value, contains := kv.kvs[args.Key]
	// kv.mu.Unlock()
	if !contains {
		reply.Value = ""
	} else {
		reply.Value = value
	}
	// kv.UpdateClerkState(args.ClerkMeta, reply.Value)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.CheckDuplicateRequest(args.ClerkMeta, &reply.Value) {
		return
	}

	kv.kvs[args.Key] = args.Value
	// kv.mu.Unlock()
	reply.Value = args.Value
	kv.UpdateClerkState(args.ClerkMeta, reply.Value)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.CheckDuplicateRequest(args.ClerkMeta, &reply.Value) {
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
	kv.UpdateClerkState(args.ClerkMeta, reply.Value)
}

func StartKVServer() *KVServer {
	// You may need initialization code here.
	return &KVServer{mu: sync.Mutex{}, kvs: make(map[string]string), buff: make(map[int64]ResponseBuffer)}
}
