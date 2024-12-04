package shardctrler

import (
	"fmt"
	"runtime"
	"sync"

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

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	debug bool
}

type Op struct {
	// Your data here.
}

func (sc *ShardCtrler) lock(lock *sync.Mutex, sign ...string) {
	lock.Lock()
	if sc.debug {
		_, _, line, _ := runtime.Caller(1)
		if len(sign) > 0 {
			DPrintf("server %d lock %v at %v\n", sc.me, sign[0], line)
		} else {
			DPrintf("server %d lock mu at %v\n", sc.me, line)
		}
	}
}

func (sc *ShardCtrler) unlock(lock *sync.Mutex, sign ...string) {
	lock.Unlock()
	if sc.debug {
		_, _, line, _ := runtime.Caller(1)
		if len(sign) > 0 {
			DPrintf("server %d unlock %v at %v\n", sc.me, sign[0], line)
		} else {
			DPrintf("server %d unlock mu at %v\n", sc.me, line)
		}
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.lock(&sc.mu)
	defer sc.unlock(&sc.mu)

	originalConfig := &sc.configs[len(sc.configs)-1]

	newConfig := Config{}
	newConfig.Num = len(sc.configs)
	newConfig.Groups = make(map[int][]string)

	// union original groups and new groups
	for gid, servers := range originalConfig.Groups {
		newConfig.Groups[gid] = servers
	}
	for gid, servers := range args.Servers {
		newConfig.Groups[gid] = servers
	}

	// cal shard count for each group
	totalGroups := len(originalConfig.Groups) + len(args.Servers)
	div := NShards / totalGroups
	mod := NShards % totalGroups

	// cal how many shards need to be trimmed for original group
	gids := make(map[int]int, 0)
	for _, gid := range originalConfig.Shards {
		gids[gid]++
	}

	// trimmmed[i] > 0 means how many shards need to be trimmed for group i
	// trimmed[i] < 0 means how many shards need to be assigned to group i
	trimmed := make(map[int]int, 0)
	for gid, count := range gids {
		if count > div && mod > 0 {
			trimmed[gid] = count - (div + 1)
			gids[gid] = div + 1
			mod--
		} else {
			trimmed[gid] = count - div
			gids[gid] = div
		}
	}

	for gid, count := range gids {
		if mod == 0 {
			break
		}
		if count == div {
			gids[gid]++
			trimmed[gid]--
			mod--
		}
	}

	// rebalance shards for groups

	// assign trimmed shards to new groups
	newGids := make([]int, 0)
	for gid := range args.Servers {
		newGids = append(newGids, gid)
	}

	groupCount := len(newGids)
	for i, j := 0, 0; i < NShards; i++ {
		gid := originalConfig.Shards[i]
		if gids[gid] > 0 {
			gids[gid]--
			newConfig.Shards[i] = newGids[j]
			j++
			if j == groupCount {
				j = 0
			}
		} else {
			newConfig.Shards[i] = gid
		}
	}

	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.lock(&sc.mu)
	defer sc.unlock(&sc.mu)

	originalConfig := &sc.configs[len(sc.configs)-1]

	newConfig := Config{}
	newConfig.Num = len(sc.configs)
	newConfig.Groups = make(map[int][]string)

	// fliter out leaved groups
	leavedGroups := make(map[int]bool)
	for _, gid := range args.GIDs {
		leavedGroups[gid] = true
	}

	gids := make(map[int]int, 0)
	for gid := range originalConfig.Groups {
		if _, ok := leavedGroups[gid]; !ok {
			gids[gid] = 0
			newConfig.Groups[gid] = originalConfig.Groups[gid]
		}
	}

	// divide zombie shards evenly to groups
	zombie := make([]int, 0)
	for i, gid := range originalConfig.Shards {
		if _, ok := leavedGroups[gid]; !ok {
			gids[gid]++
			newConfig.Shards[i] = gid
		} else {
			zombie = append(zombie, i)
		}
	}

	for shard := range zombie {
		minGid := -1
		minCount := NShards + 1
		for gid, count := range gids {
			if count < minCount {
				minCount = count
				minGid = gid
			}
		}
		newConfig.Shards[shard] = minGid
		gids[minGid]++
	}

	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.lock(&sc.mu)
	defer sc.unlock(&sc.mu)

	originalConfig := &sc.configs[len(sc.configs)-1]
	newConfig := Config{}

	newConfig.Num = len(sc.configs)
	newConfig.Groups = make(map[int][]string)
	for key, value := range originalConfig.Groups {
		newConfig.Groups[key] = value
	}

	// copy shards from original config
	newConfig.Shards = originalConfig.Shards

	// move shard to new group
	newConfig.Shards[args.Shard] = args.GID
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.lock(&sc.mu)
	defer sc.unlock(&sc.mu)

	if args.Num >= len(sc.configs) || args.Num < 0 {
		reply.Config = sc.configs[len(sc.configs)-1]
	} else {
		reply.Config = sc.configs[args.Num]
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.debug = false

	return sc
}
