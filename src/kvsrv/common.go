package kvsrv

type MetaInfo struct {
	// Your definitions here.
	ClerkId   int64
	RequestId int64
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkMeta MetaInfo
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkMeta MetaInfo
}

type GetReply struct {
	Value string
}
