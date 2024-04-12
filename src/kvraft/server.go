package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// ------------------------------------------------ 结构体定义 -----------------------------------------------------------

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SeqId    int
	Key      string
	Value    string
	ClientId int64
	Index    int // raft服务层转来的Index
	OpType   string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	seqMap    map[int64]int     //保证每个客户端命令只执行一次, clientId -> maxSeqId
	waitChMap map[int]chan Op   //
	kvPersist map[string]string // 用来存储键值对

	lastIncludeIndex int //
}

// --------------------------------------------- 初始化 --------------------------------------------------------------

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.seqMap = make(map[int64]int)
	kv.kvPersist = make(map[string]string)
	kv.waitChMap = make(map[int]chan Op)

	kv.lastIncludeIndex = -1

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapshot(snapshot)
	}

	go kv.applyMsgHandlerLoop()
	return kv

}

// -------------------------------------------- Rpc部分 --------------------------------------------------------------

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 封装Op到下称Start
	op := Op{
		OpType:   "Get",
		Key:      args.Key,
		SeqId:    args.SeqId,
		ClientId: args.ClientId,
	}
	lastIndex, _, _ := kv.rf.Start(op) // 得到raft层的index

	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, lastIndex)
		kv.mu.Unlock()
	}()

	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch: // 来自raft层对index的提交结果
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.kvPersist[args.Key]
			kv.mu.Unlock()
			return
		}

	case <-timer.C: //超时也返回
		reply.Err = ErrWrongLeader
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 封装Op到下称Start
	op := Op{
		OpType:   args.Op,
		Key:      args.Key,
		Value:    args.Value,
		SeqId:    args.SeqId,
		ClientId: args.ClientId,
	}

	lastIndex, _, _ := kv.rf.Start(op)
	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, lastIndex)
		kv.mu.Unlock()
	}()

	timer := time.NewTicker(100 * time.Millisecond)

	select {
	case replyOp := <-ch:
		//DPrintf("PutAppend成功...")
		// 通过clientId、seqId确定唯一操作序列
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {

			reply.Err = OK
		}
	case <-timer.C:
		//DPrintf("PutAppend超时..")
		reply.Err = ErrWrongLeader
	}
	defer timer.Stop()
}

// -----------------------------------------Loop部分----------------------------------------------------------------

//处理applyCh发送过来的AppMsg

func (kv *KVServer) applyMsgHandlerLoop() {
	for {
		if kv.killed() {
			return
		}
		select {
		case msg := <-kv.applyCh: //等待raft提交日志
			if msg.CommandValid {
				// 传来的信息快照已经存储了
				if msg.CommandIndex <= kv.lastIncludeIndex {
					return
				}

				index := msg.CommandIndex
				op := msg.Command.(Op)

				if !kv.ifDuplicate(op.ClientId, op.SeqId) {
					kv.mu.Lock()
					switch op.OpType {
					case "Put":
						kv.kvPersist[op.Key] = op.Value
					case "Append":
						kv.kvPersist[op.Key] += op.Value
						DPrintf("%v", op)
						DPrintf("%v", kv.kvPersist)
					}
					kv.seqMap[op.ClientId] = op.SeqId
					kv.mu.Unlock()
				}

				//
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
					snapshot := kv.PersistSnapshot()
					kv.rf.SnapShot(msg.CommandIndex, snapshot)
				}

				kv.getWaitCh(index) <- op //将结果返回到接口层
				//DPrintf("Sever[%v] 提交日志：【%v, %v, %v】\n", kv.me, msg.CommandIndex, msg.Term, msg.Command)

			}
			if msg.SnapshotValid {
				kv.mu.Lock()
				// 判断此时有没有竞争
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					// 读取快照的数据
					kv.DecodeSnapshot(msg.Snapshot)
					kv.lastIncludeIndex = msg.SnapshotIndex
				}
				kv.mu.Unlock()
			}
		}
	}
}

// ----------------------------------------- 持久化部分 --------------------------------------------------------------

func (kv *KVServer) DecodeSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvPersist map[string]string
	var seqMap map[int64]int

	if d.Decode(&kvPersist) == nil && d.Decode(&seqMap) == nil {
		kv.kvPersist = kvPersist
		kv.seqMap = seqMap
	} else {
		fmt.Printf("[Server(%v)] Failed to decode snapshot！！！", kv.me)
	}

}

func (kv *KVServer) PersistSnapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvPersist)
	e.Encode(kv.seqMap)
	data := w.Bytes()
	return data
}

// -------------------------------------------------- utils部分 ------------------------------------------------------

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) ifDuplicate(clientId int64, seqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastSeqId, exist := kv.seqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId

}

func (kv *KVServer) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan Op, 1)
		ch = kv.waitChMap[index]
	}
	return ch
}
