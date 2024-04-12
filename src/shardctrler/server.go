package shardctrler

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"time"
)
import "sync"

const (
	JoinType  = "join"
	LeaveType = "leave"
	MoveType  = "move"
	QueryType = "query"

	JoinOverTime  = 100
	LeaveOverTime = 100
	MoveOverTime  = 100
	QueryOverTime = 100

	InvalidGid = 0
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	seqMap    map[int64]int   //重复性检查,存储Client最大的SeqId
	waitChMap map[int]chan Op //对特定Index日志的提交检测
}

type Op struct {
	// Your data here.
	OpType      string
	ClientId    int64
	SeqId       int
	QueryNum    int
	JoinServers map[int][]string
	LeaveGids   []int
	MoveShard   int
	MoveGid     int
}

// ------------------------------------------------------初始化部分------------------------------------------------------

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.seqMap = make(map[int64]int)
	sc.waitChMap = make(map[int]chan Op)

	go sc.applyMsgHandler()

	return sc
}

// --------------------------------------------------------Rpc部分------------------------------------------------------

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		OpType:      JoinType,
		SeqId:       args.SeqId,
		ClientId:    args.ClientId,
		JoinServers: args.Servers,
	}

	lastIndex, _, _ := sc.rf.Start(op)
	DPrintf("等待Client[%v, %v]的Join请求完成...", op.ClientId, op.SeqId)
	ch := sc.getWaitCh(lastIndex)

	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex)
		sc.mu.Unlock()
	}()

	timer := time.NewTicker(JoinOverTime * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if replyOp.ClientId != args.ClientId || replyOp.SeqId != args.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = Ok
			DPrintf("完成后结果：%v", sc.configs[len(sc.configs)-1])
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader

	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		OpType:    LeaveType,
		SeqId:     args.SeqId,
		ClientId:  args.ClientId,
		LeaveGids: args.GIDs,
	}

	lastIndex, _, _ := sc.rf.Start(op)

	ch := sc.getWaitCh(lastIndex)

	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex)
		sc.mu.Unlock()
	}()

	timer := time.NewTicker(JoinOverTime * time.Millisecond)
	defer timer.Stop()

	DPrintf("等待Client[%v, %v]的Leave请求完成...", op.ClientId, op.SeqId)
	select {
	case replyOp := <-ch:
		if replyOp.ClientId != args.ClientId || replyOp.SeqId != args.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = Ok
			DPrintf("完成后结果：%v", sc.configs[len(sc.configs)-1])
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader

	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		OpType:    MoveType,
		SeqId:     args.SeqId,
		ClientId:  args.ClientId,
		MoveGid:   args.GID,
		MoveShard: args.Shard,
	}

	lastIndex, _, _ := sc.rf.Start(op)

	ch := sc.getWaitCh(lastIndex)

	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex)
		sc.mu.Unlock()
	}()

	timer := time.NewTicker(JoinOverTime * time.Millisecond)
	defer timer.Stop()
	DPrintf("等待Client[%v, %v]的Move请求完成...", op.ClientId, op.SeqId)
	select {
	case replyOp := <-ch:
		if replyOp.ClientId != args.ClientId || replyOp.SeqId != args.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = Ok
			DPrintf("完成后结果：%v", sc.configs[len(sc.configs)-1])
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader

	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		OpType:   QueryType,
		SeqId:    args.SeqId,
		ClientId: args.ClientId,
		QueryNum: args.Num,
	}

	lastIndex, _, _ := sc.rf.Start(op)
	DPrintf("等待Client[%v, %v]的Query请求完成...", op.ClientId, op.SeqId)
	ch := sc.getWaitCh(lastIndex)

	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex)
		sc.mu.Unlock()
	}()

	timer := time.NewTicker(JoinOverTime * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if replyOp.ClientId != args.ClientId || replyOp.SeqId != args.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			sc.mu.Lock()
			sc.seqMap[op.ClientId] = op.SeqId
			if op.QueryNum == -1 || op.QueryNum >= len(sc.configs) {
				reply.Config = sc.configs[len(sc.configs)-1]
			} else {
				reply.Config = sc.configs[op.QueryNum]
			}
			reply.Err = Ok
			sc.mu.Unlock()
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader

	}
}

// -------------------------------------------------Loop 部分-----------------------------------------------------------

func (sc *ShardCtrler) applyMsgHandler() {
	for {
		select {
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				index := msg.CommandIndex
				op := msg.Command.(Op)

				if !sc.ifDuplicate(op.ClientId, op.SeqId) {
					sc.mu.Lock()
					switch op.OpType {
					case JoinType:
						sc.seqMap[op.ClientId] = op.SeqId
						sc.configs = append(sc.configs, *sc.JoinHandler(op.JoinServers))
					case LeaveType:
						sc.seqMap[op.ClientId] = op.SeqId
						sc.configs = append(sc.configs, *sc.LeaveHandler(op.LeaveGids))
					case MoveType:
						sc.seqMap[op.ClientId] = op.SeqId
						sc.configs = append(sc.configs, *sc.moveHandler(op.MoveGid, op.MoveShard))
					}
					sc.seqMap[op.ClientId] = op.SeqId
					sc.mu.Unlock()
				}

				sc.getWaitCh(index) <- op
			}

		}

	}

}

// ------------------------------------------------handler部分--------------------------------------------------------

func (sc *ShardCtrler) JoinHandler(servers map[int][]string) *Config {
	//去除最后一个分组将 新分组加入
	lastConfig := sc.configs[len(sc.configs)-1]
	newGroups := make(map[int][]string)

	for gid, serverList := range lastConfig.Groups {
		newGroups[gid] = serverList
	}

	for gid, serverList := range servers {
		newGroups[gid] = serverList
	}

	//统计每隔二group有几个shard
	groupSharCount := make(map[int]int)
	for gid := range newGroups {
		groupSharCount[gid] = 0
	}

	for _, gid := range lastConfig.Shards {
		if gid != 0 {
			groupSharCount[gid]++
		}
	}

	if len(groupSharCount) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [NShards]int{},
			Groups: newGroups,
		}
	}

	return &Config{
		Num:    len(sc.configs),
		Shards: sc.loadBalance(groupSharCount, lastConfig.Shards),
		Groups: newGroups,
	}

}

func (sc *ShardCtrler) LeaveHandler(gids []int) *Config {
	leaveMap := make(map[int]bool)
	for _, gid := range gids {
		leaveMap[gid] = true
	}

	lastConfig := sc.configs[len(sc.configs)-1]
	newGroup := make(map[int][]string)

	for gid, serverList := range lastConfig.Groups {
		newGroup[gid] = serverList
	}
	for gid, _ := range leaveMap {
		delete(newGroup, gid)
	}

	groupShardCount := make(map[int]int)
	newShard := lastConfig.Shards

	for gid := range newGroup {
		groupShardCount[gid] = 0
	}

	for shard, gid := range lastConfig.Shards {
		if gid != 0 {
			if leaveMap[gid] {
				newShard[shard] = 0
			} else {
				groupShardCount[gid]++
			}
		}
	}

	if len(groupShardCount) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [NShards]int{},
			Groups: newGroup,
		}
	}

	return &Config{
		Num:    len(sc.configs),
		Shards: sc.loadBalance(groupShardCount, newShard),
		Groups: newGroup,
	}

}

func (sc *ShardCtrler) moveHandler(gid int, shard int) *Config {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    len(sc.configs),
		Shards: [NShards]int{},
		Groups: map[int][]string{},
	}

	for shd, gd := range lastConfig.Shards {
		newConfig.Shards[shd] = gd
	}
	newConfig.Shards[shard] = gid

	for gd, servers := range lastConfig.Groups {
		newConfig.Groups[gd] = servers
	}
	return &newConfig
}

// ------------------------------------------------utils部分-----------------------------------------------------------

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//

func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester

func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) ifDuplicate(clientId int64, seqId int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	lastSeqId, exist := sc.seqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId

}

func (sc *ShardCtrler) getWaitCh(index int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, exist := sc.waitChMap[index]
	if !exist {
		sc.waitChMap[index] = make(chan Op, 1)
		ch = sc.waitChMap[index]
	}
	return ch
}

func (sc *ShardCtrler) loadBalance(groupShardCount map[int]int, lastShard [NShards]int) [NShards]int {
	length := len(groupShardCount) //Group数量
	ave := NShards / length
	remainder := NShards % length
	sortGids := sortGroupShard(groupShardCount) //这里排序的原因是，保证每个server再做负载均衡时都得到一样的结果，因为Map遍历的顺序是不一致的

	// 将多余负载free
	for i := 0; i < length; i++ {
		target := ave

		if moreAllocations(length, remainder, i) {
			target = ave + 1
		}

		if groupShardCount[sortGids[i]] > target {
			overLoadGid := sortGids[i]
			changeNum := groupShardCount[overLoadGid] - target
			for shard, gid := range lastShard {
				if changeNum <= 0 {
					break
				}
				if gid == overLoadGid {
					lastShard[shard] = InvalidGid //free
					changeNum--
				}
			}
			groupShardCount[overLoadGid] = target
		}
	}
	// 为负载少的group分配多出的负载

	for i := 0; i < length; i++ {
		target := ave
		if moreAllocations(length, remainder, i) {
			target = ave + 1
		}

		if groupShardCount[sortGids[i]] < target {
			freeGid := sortGids[i]
			changeNum := target - groupShardCount[freeGid]

			for shard, gid := range lastShard {
				if changeNum <= 0 {
					break
				}
				if gid == InvalidGid {
					lastShard[shard] = freeGid
					changeNum--
				}
			}
			groupShardCount[freeGid] = target
		}
	}
	return lastShard
}

func sortGroupShard(groupShardCount map[int]int) []int {
	length := len(groupShardCount)

	gidSlice := make([]int, 0, length)

	for gid, _ := range groupShardCount {
		gidSlice = append(gidSlice, gid)
	}

	for i := 0; i < length-1; i++ {
		for j := length - 1; j > i; j-- {
			if groupShardCount[gidSlice[j]] > groupShardCount[gidSlice[j-1]] || (groupShardCount[gidSlice[j]] == groupShardCount[gidSlice[j-1]] && gidSlice[j] < gidSlice[j-1]) {
				gidSlice[j], gidSlice[j-1] = gidSlice[j-1], gidSlice[j]
			}
		}
	}
	return gidSlice
}

func moreAllocations(length int, remainder int, i int) bool {
	if i < length-remainder {
		return false
	} else {
		return true
	}
}
