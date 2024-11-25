package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id        int64
	requestId int
	leaderId  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = nrand()
	ck.requestId = 0
	ck.leaderId = 0 // client assumes default leader is 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{Key: key, ClerkMeta: MetaAppendInfo{ClerkId: ck.id, RequestId: ck.requestId}}
	var reply GetReply

loop:
	for i := ck.leaderId; ; i++ {
		if i == len(ck.servers) {
			i = 0
		}

		DPrintf("Client %v Get key %s to %v\n", ck.id, key, i)
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		DPrintf("Client %v Get key %s to %v ok %v reply %v\n", ck.id, key, ck.leaderId, ok, reply)
		if !ok {
			continue
		}

		switch reply.Err {
		case OK:
			ck.leaderId = i
			break loop
		case ErrWrongLeader:
			continue
		case ErrNoAgreement:
			i-- // retry current leader
			continue
		case ErrKilledServer:
			break loop
		}
	}
	ck.requestId++
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{Key: key, Value: value, ClerkMeta: MetaAppendInfo{ClerkId: ck.id, RequestId: ck.requestId}}
	var reply PutAppendReply

loop:
	for i := ck.leaderId; ; i++ {
		if i == len(ck.servers) {
			i = 0
		}

		DPrintf("Client %v %s key %s value %s to %v\n", ck.id, op, key, value, i)

		ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
		DPrintf("Client %v %s key %s value %s to %v reply ok %v reply %v\n", ck.id, op, key, value, i, ok, reply)
		if !ok {
			continue
		}

		switch reply.Err {
		case OK:
			ck.leaderId = i
			break loop
		case ErrWrongLeader:
			continue
		case ErrNoAgreement:
			i-- // retry current leader
			continue
		case ErrKilledServer:
			break loop
		}
	}

	DPrintf("Client %v %s key %s value %s to %v success reply %v\n", ck.id, op, key, value, ck.leaderId, reply)
	ck.requestId++
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
