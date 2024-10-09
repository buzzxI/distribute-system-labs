package kvsrv

type MetaAppendInfo struct {
	// Your definitions here.
	ClerkId   int64
	RequestId bool // one client for a clerk, a bool request id is enough
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkMeta MetaAppendInfo
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}
