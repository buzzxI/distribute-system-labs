package kvraft

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrNoAgreement  = "ErrNoAgreement"
	ErrKilledServer = "ErrKilledServer"
)

type Err string

type MetaAppendInfo struct {
	ClerkId   int64
	RequestId int
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
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkMeta MetaAppendInfo
}

type GetReply struct {
	Err   Err
	Value string
}
