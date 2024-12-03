package shardctrler

import (
	"sync"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	originalConfig := &sc.configs[len(sc.configs)-1]

	newConfig := Config{}
	newConfig.Num = len(sc.configs)
	newConfig.Groups = make(map[int][]string)

	totalGroups := len(originalConfig.Groups) + len(args.Servers)
	for gid, servers := range originalConfig.Groups {
		newConfig.Groups[gid] = servers
	}
	for gid, servers := range args.Servers {
		newConfig.Groups[gid] = servers
	}

	// cal shard count for each group
	div := NShards / totalGroups
	mod := NShards % totalGroups

	gids := make(map[int]int, 0)
	for _, gid := range originalConfig.Shards {
		gids[gid]++
	}

	// cal how many shards need to be trimmed for original group
	trimCount := make(map[int]int, 0)
	for gid, count := range gids {
		if count > div {
			if mod > 0 {
				trimCount[gid] = count - (div + 1)
				mod--
			} else {
				trimCount[gid] = count - div
			}
		}
	}

	newGids := make([]int, 0)
	for gid := range args.Servers {
		newGids = append(newGids, gid)
	}

	// assign trimmed shards to new groups
	groupCount := len(newGids)
	for i, j := 0, 0; i < NShards; i++ {
		gid := originalConfig.Shards[i]
		if trimCount[gid] > 0 {
			trimCount[gid]--
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
	sc.mu.Lock()
	defer sc.mu.Unlock()

	originalConfig := &sc.configs[len(sc.configs)-1]

	newConfig := Config{}
	newConfig.Num = len(sc.configs)

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
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
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

	return sc
}
