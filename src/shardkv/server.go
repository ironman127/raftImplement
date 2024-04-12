package shardkv

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"

// ------------------------------------------------结构体定义部分------------------------------------------------------

const (
	UpConfigLoopInterval = 100 * time.Millisecond

	GetTimeOut          = 500 * time.Millisecond
	AppOrPutTimeOut     = 500 * time.Millisecond
	UpConfigTimeOut     = 500 * time.Millisecond
	AddShardsTimeOut    = 500 * time.Millisecond
	RemoveShardsTimeOut = 500 * time.Millisecond
)

type Shard struct {
	KvMap     map[string]string //Shard中存储的对象
	ConfigNum int
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	SeqId    int
	OpType   Operation
	Key      string
	Value    string
	UpConfig shardctrler.Config
	ShardId  int
	Shard    Shard
	SeqMap   map[int64]int
}

type OpReply struct {
	ClientId int64
	SeqId    int
	Err      Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	makeEnd      func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxRaftState int // snapshot if log grows this big

	// Your definitions here.
	dead int32

	Config     shardctrler.Config
	LastConfig shardctrler.Config

	shardPersist []Shard // ShardId -> Shard 如果KvMap = nil说明当前数据分片不贵此服务器管
	waitChMap    map[int]chan OpReply
	SeqMap       map[int64]int //防止重复操作
	sck          *shardctrler.Clerk
}

// ---------------------------------------------初始化部分-----------------------------------------------------------

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxRaftState = maxraftstate
	kv.makeEnd = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.shardPersist = make([]Shard, shardctrler.NShards)
	kv.SeqMap = make(map[int64]int)

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.sck = shardctrler.MakeClerk(kv.masters)
	kv.waitChMap = make(map[int]chan OpReply)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyMsgHandlerLoop()
	go kv.configDetectedLoop()

	return kv
}

// --------------------------------------------------Loop部分---------------------------------------------------------

func (kv *ShardKV) applyMsgHandlerLoop() {
	for {
		if kv.killed() {
			return
		}

		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid == true {
				kv.mu.Lock()
				op := msg.Command.(Op)
				reply := OpReply{
					ClientId: op.ClientId,
					SeqId:    op.SeqId,
					Err:      OK,
				}

				if op.OpType == PutType || op.OpType == GetType || op.OpType == AppendType {
					shardId := key2shard(op.Key)

					if kv.Config.Shards[shardId] != kv.gid {
						reply.Err = ErrWrongGroup
					} else if kv.shardPersist[shardId].KvMap == nil {
						reply.Err = ShardNotArrived
					} else {
						if !kv.ifDuplicate(op.ClientId, op.SeqId) {
							kv.SeqMap[op.ClientId] = op.SeqId
							switch op.OpType {
							case PutType:
								kv.shardPersist[shardId].KvMap[op.Key] = op.Value
								DPrintf("Server [%v, %v] After Put: %v", kv.gid, kv.me, kv.shardPersist)
							case AppendType:
								kv.shardPersist[shardId].KvMap[op.Key] += op.Value
								DPrintf("Server [%v, %v] After Append: %v", kv.gid, kv.me, kv.shardPersist)
							case GetType:
							default:
								log.Fatalf("invalid command type: %v", op.OpType)
							}

						}
					}
				} else {
					switch op.OpType {
					case UpConfigType:
						kv.upConfigHandler(op)
					case AddShardType:

						if kv.Config.Num < op.SeqId {
							reply.Err = ConfigNotArrived
							break
						}
						kv.addShardHandler(op)
						DPrintf("Server [%v, %v] After AddShard: %v", kv.gid, kv.me, kv.shardPersist)
					case RemoveShardType:
						kv.removeShardHandler(op)
						DPrintf("Server [%v, %v] After MoveShard: %v", kv.gid, kv.me, kv.shardPersist)
					default:
						log.Fatalf("invalid command type: %v", op.OpType)
					}

				}

				if kv.maxRaftState != -1 && kv.rf.GetRaftStateSize() > kv.maxRaftState {
					snapshot := kv.PersistSnapShot()
					kv.rf.SnapShot(msg.CommandIndex, snapshot)
				}

				ch := kv.getWaitCh(msg.CommandIndex)
				ch <- reply
				kv.mu.Unlock()
			}

			if msg.SnapshotValid == true {
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.mu.Lock()
					kv.DecodeSnapShot(msg.Snapshot)
					kv.mu.Unlock()
				}
				continue
			}
		}

	}
}

func (kv *ShardKV) configDetectedLoop() {
	kv.mu.Lock()
	curConfig := kv.Config
	rf := kv.rf
	kv.mu.Unlock()

	for !kv.killed() {
		if _, isLeader := rf.GetState(); !isLeader {
			time.Sleep(UpConfigLoopInterval)
			continue
		}
		kv.mu.Lock()
		DPrintf("Server[%v, %v]最新配置为: [%v]", kv.gid, kv.me, kv.Config)
		//DPrintf("Server[%v, %v]上一次配置为： [%v]", kv.gid, kv.me, kv.LastConfig)
		// 判断是否把不属于自己的部分发送给了别人
		if !kv.allSent() {
			SeqMap := make(map[int64]int)
			for k, v := range kv.SeqMap {
				SeqMap[k] = v
			}

			for shardId, gid := range kv.LastConfig.Shards {
				if gid == kv.gid && kv.Config.Shards[shardId] != kv.gid && kv.shardPersist[shardId].ConfigNum < kv.Config.Num {

					sendData := kv.cloneShard(kv.Config.Num, kv.shardPersist[shardId].KvMap)

					args := SendShardArg{
						LastAppliedRequestId: SeqMap,
						ShardId:              shardId,
						ClientId:             int64(gid),
						RequestId:            kv.Config.Num,
						Shard:                sendData,
					}

					serverList := kv.Config.Groups[kv.Config.Shards[shardId]]
					servers := make([]*labrpc.ClientEnd, len(serverList))

					for i, name := range serverList {
						servers[i] = kv.makeEnd(name)
					}

					// 开启协程对每个客户端发送切片
					go func(servers []*labrpc.ClientEnd, args *SendShardArg) {
						index := 0
						start := time.Now()
						for {
							var reply AddShardReply

							ok := servers[index].Call("ShardKV.AddShard", args, &reply)

							if ok && reply.Err == OK || time.Now().Sub(start) >= 2*time.Second {
								kv.mu.Lock()

								command := Op{
									OpType:   RemoveShardType,
									ClientId: int64(kv.gid),
									SeqId:    kv.Config.Num,
									ShardId:  args.ShardId,
								}
								kv.mu.Unlock()
								kv.startCommand(command, RemoveShardsTimeOut)
								break
							}
							index = (index + 1) % len(servers)
							if index == 0 {
								time.Sleep(UpConfigLoopInterval)
							}
						}
					}(servers, &args)

				}
			}

			kv.mu.Unlock()
			time.Sleep(UpConfigLoopInterval)
			continue
		}
		if !kv.allReceived() {
			kv.mu.Unlock()
			time.Sleep(UpConfigTimeOut)
			continue
		}
		//DPrintf("============================================Server[%v, %v]更新配置成================================================", kv.gid, kv.me)
		curConfig = kv.Config
		sck := kv.sck
		kv.mu.Unlock()

		newConfig := sck.Query(curConfig.Num + 1)

		if newConfig.Num != curConfig.Num+1 {
			time.Sleep(UpConfigLoopInterval)
			continue
		}

		command := Op{
			OpType:   UpConfigType,
			ClientId: int64(kv.gid),
			SeqId:    newConfig.Num,
			UpConfig: newConfig,
		}
		kv.startCommand(command, UpConfigTimeOut)

	}

}

//----------------------------------------------------------Rpc部分------------------------------------------------------

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardPersist[shardId].KvMap == nil {
		reply.Err = ShardNotArrived
	}
	kv.mu.Unlock()
	if reply.Err == ErrWrongGroup || reply.Err == ShardNotArrived {
		return
	}
	command := Op{
		OpType:   GetType,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
		Key:      args.Key,
	}
	DPrintf("Sever[%v, %v]接收到Client[%v, %v]的Get请求: [%v]", kv.gid, kv.me, args.ClientId, args.RequestId, args.Key)
	err := kv.startCommand(command, GetTimeOut)
	if err != OK {
		reply.Err = err
		return
	}
	kv.mu.Lock()

	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardPersist[shardId].KvMap == nil {
		reply.Err = ShardNotArrived
	} else {
		reply.Err = OK
		reply.Value = kv.shardPersist[shardId].KvMap[args.Key]
	}
	kv.mu.Unlock()
	return

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	shardId := key2shard(args.Key)

	kv.mu.Lock()
	DPrintf("Sever[%v, %v]接收到Client[%v, %v]的%v请求: [%v, %v]", kv.gid, kv.me, args.ClientId, args.RequestId, args.Op, args.Key, args.Value)

	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardPersist[shardId].KvMap == nil {
		reply.Err = ShardNotArrived
	}
	kv.mu.Unlock()
	if reply.Err == ErrWrongGroup || reply.Err == ShardNotArrived {
		return
	}
	command := Op{
		OpType:   args.Op,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
		Key:      args.Key,
		Value:    args.Value,
	}

	reply.Err = kv.startCommand(command, AppOrPutTimeOut)
	return
}

func (kv *ShardKV) AddShard(args *SendShardArg, reply *AddShardReply) {
	command := Op{
		OpType:   AddShardType,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
		ShardId:  args.ShardId,
		Shard:    args.Shard,
		SeqMap:   args.LastAppliedRequestId,
	}
	DPrintf("Sever[%v, %v]接收到Client[%v, %v]的AddShard请求: [%v] - [%d]", kv.gid, kv.me, args.ClientId, args.RequestId, args.Shard, args.ShardId)
	reply.Err = kv.startCommand(command, AddShardsTimeOut)
	return
}

// ---------------------------------------------- Handler部分 ------------------------------------------------------

// 更新config的handler
func (kv *ShardKV) upConfigHandler(op Op) {
	curConfig := kv.Config
	upConfig := op.UpConfig

	if curConfig.Num >= upConfig.Num {
		return
	}
	//检查本组是否更新了新的分片
	for shard, gid := range upConfig.Shards {
		if gid == kv.gid && curConfig.Shards[shard] == 0 {
			kv.shardPersist[shard].KvMap = make(map[string]string)
			kv.shardPersist[shard].ConfigNum = upConfig.Num
		}
	}
	kv.LastConfig = curConfig
	kv.Config = upConfig
}

func (kv *ShardKV) addShardHandler(op Op) {
	if kv.shardPersist[op.ShardId].KvMap != nil || op.Shard.ConfigNum < kv.Config.Num {
		DPrintf("Server[%v, %v] addShard [%v] - [%v] 失败....", kv.gid, kv.me, kv.shardPersist[op.ShardId].KvMap, kv.Config.Num)
		return
	}
	kv.shardPersist[op.ShardId] = kv.cloneShard(op.Shard.ConfigNum, op.Shard.KvMap)

	for clientId, seqId := range op.SeqMap {
		if r, ok := kv.SeqMap[clientId]; !ok || r < seqId {
			kv.SeqMap[clientId] = seqId
		}
	}
	DPrintf("Server[%v, %v] addShard [%v] - [%v] 成功....", kv.gid, kv.me, op.Shard, op.ShardId)
}

func (kv *ShardKV) removeShardHandler(op Op) {
	if op.SeqId < kv.Config.Num {
		return
	}
	DPrintf("Server[%v, %v] removeShard [%v] - [%v] 成功....", kv.gid, kv.me, op.Shard, op.ShardId)
	kv.shardPersist[op.ShardId].KvMap = nil
	kv.shardPersist[op.ShardId].ConfigNum = op.SeqId
}

// -----------------------------------------------持久化部分---------------------------------------------------------

func (kv *ShardKV) PersistSnapShot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(kv.shardPersist)
	err = e.Encode(kv.SeqMap)
	err = e.Encode(kv.maxRaftState)
	err = e.Encode(kv.Config)
	err = e.Encode(kv.LastConfig)
	if err != nil {
		log.Fatalf("[%d - %d] fail to snapshot.", kv.gid, kv.me)
	}
	data := w.Bytes()
	return data
}

func (kv *ShardKV) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var shardPersist []Shard
	var SeqMap map[int64]int
	var MaxRaftState int
	var Config, LastConfig shardctrler.Config

	if d.Decode(&shardPersist) != nil || d.Decode(&SeqMap) != nil ||
		d.Decode(&MaxRaftState) != nil || d.Decode(&Config) != nil ||
		d.Decode(&LastConfig) != nil {
		log.Fatalf("[%d - %d] fail to decode snapshot.", kv.gid, kv.me)
	} else {
		kv.shardPersist = shardPersist
		kv.SeqMap = SeqMap
		kv.maxRaftState = MaxRaftState
		kv.Config = Config
		kv.LastConfig = LastConfig
	}

}

// ------------------------------------------------utils部分----------------------------------------------------------

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) ifDuplicate(clientId int64, seqId int) bool {

	lastSeqId, exist := kv.SeqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId

}

func (kv *ShardKV) getWaitCh(index int) chan OpReply {

	ch, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan OpReply, 1)
		ch = kv.waitChMap[index]
	}
	return ch
}

func (kv *ShardKV) cloneShard(configNum int, kvMap map[string]string) Shard {
	migrateShard := Shard{
		KvMap:     make(map[string]string),
		ConfigNum: configNum,
	}

	for k, v := range kvMap {
		migrateShard.KvMap[k] = v
	}
	return migrateShard
}

func (kv *ShardKV) allSent() bool {
	for shard, gid := range kv.LastConfig.Shards {
		if gid == kv.gid && kv.Config.Shards[shard] != kv.gid && kv.shardPersist[shard].ConfigNum < kv.Config.Num {
			return false
		}
	}
	//DPrintf("==================================Server[%v, %v]全部发送成功=================================", kv.gid, kv.me)
	return true
}

func (kv *ShardKV) allReceived() bool {
	for shard, gid := range kv.LastConfig.Shards {
		if gid != kv.gid && kv.Config.Shards[shard] == kv.gid && kv.shardPersist[shard].ConfigNum < kv.Config.Num {
			return false
		}
	}
	//DPrintf("==================================Server[%v, %v]全部接收成功=================================", kv.gid, kv.me)
	return true
}

func (kv *ShardKV) startCommand(command Op, timeoutPeriod time.Duration) Err {
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(command)

	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader
	}

	ch := kv.getWaitCh(index)
	kv.mu.Unlock()

	timer := time.NewTicker(timeoutPeriod)
	defer timer.Stop()

	select {
	case re := <-ch:
		kv.mu.Lock()
		delete(kv.waitChMap, index)
		if re.SeqId != command.SeqId || re.ClientId != command.ClientId {
			kv.mu.Unlock()
			return ErrInconsistentData
		}
		kv.mu.Unlock()
		return re.Err
	case <-timer.C:
		return ErrOverTime

	}
}
