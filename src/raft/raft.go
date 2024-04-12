package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"sync/atomic"

	//	"bytes"
	"sync"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

// Role 节点角色
type Role int

// VoteState 投票状态
type VoteState int

// AppendEntriesState  追加日志状态
type AppendEntriesState int

const ( //枚举节点角色
	Follower Role = iota
	Candidate
	Leader
)

const (

	// MoreVoteTime MinVoteTime 定义随机生成投票过期时间范围:(MoreVoteTime+MinVoteTime~MinVoteTime)
	MoreVoteTime = 100
	MinVoteTime  = 75

	// HeartbeatSleep 心脏休眠时间,要注意的是，这个时间要比选举低，才能建立稳定心跳机制
	HeartbeatSleep = 35
	AppliedSleep   = 15
)

type LogEntry struct {
	Term    int
	Command interface{}
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Term         int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	Me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//所有类型server拥有的变量
	currentTerm int        //当前任期
	votedFor    int        //记录当前节点的投票对象
	logs        []LogEntry //日志条目数组

	//所有server经常修改的
	commitIndex int //状态机中已经被提交的日志条目的索引值
	lastApplied int //最后一个被添加到状态机的索引值

	//leader拥有的变量
	nextIndex  []int //需要发送给follower下一个日志条目的索引值
	matchIndex []int //已经复制给follower的最后日志条目的下标

	role              Role //节点的角色
	reElectionTimeout time.Duration
	timer             *time.Ticker //每个节点的计时器

	//除paper以外的参数
	voteTimer time.Time
	votedNum  int //统计被投票数量

	//快照点
	lastIncludeIndex int
	lastIncludeTerm  int

	applyChan chan ApplyMsg //写入channel
}

type AppendEntriesArgs struct {
	Term         int        //任期
	LeadId       int        //leader的ID
	PrevLogIndex int        //预计从哪里追加的index，初始化为rf.nextIndex[i] - 1
	PrevLogTerm  int        //追加日志的任期号
	Entries      []LogEntry //预计存储的日志
	LeaderCommit int        //最后一个被大多数机器都复制的日志的index

}

type AppendEntriesReply struct { //follower给leader的返回结构
	Term        int
	Success     bool
	UpNextIndex int //更新Leader的nextIndex[i]
}

type RequestVoteArgs struct {
	Term         int //候选人的任期
	CandidateId  int //候选人id
	LastLogIndex int //候选人日志最后索引
	LastLogTerm  int //候选人最后索引的任期号
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //投票方的任期
	VoteGranted bool //是否投给该竞选者
}

type InstallSnapShotArgs struct {
	Term             int //发送请求方的任期
	LeaderId         int //请求方的leaderID
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type InstallSnapShotReply struct {
	Term int
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.Me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()

	rf.applyChan = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.votedNum = 0
	rf.logs = make([]LogEntry, 0)
	rf.logs = append(rf.logs, LogEntry{}) //commitIndex从零开始，为已经提交的最后一个日志

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = 0

	rf.role = Follower
	rf.reElectionTimeout = time.Duration(generateOverTime(int64(rf.Me))) * time.Millisecond
	rf.timer = time.NewTicker(rf.reElectionTimeout)
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//fmt.Printf("恢复Raft[%v]\n", rf.me)
	DPrintf("Raft[%d, %d]恢复快照 %d......", rf.Me, rf.currentTerm, rf.lastIncludeIndex)

	if rf.lastIncludeIndex > 0 {
		rf.lastApplied = rf.lastIncludeIndex
	}

	if rf.lastIncludeIndex > 0 {
		rf.commitIndex = rf.lastIncludeIndex
	}

	go rf.electionTicker()
	go rf.appendTicker()
	go rf.committedTicker()

	// start ticker goroutine to start elections

	return rf
}

// ---------------------------------------------Ticker部分--------------------------------------------------------------

func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		nowTime := time.Now()

		time.Sleep(time.Duration(generateOverTime(int64(rf.Me))) * time.Millisecond)
		rf.mu.Lock()

		// 时间过期发起选举
		if rf.voteTimer.Before(nowTime) && rf.role != Leader {
			// 变为候选者
			rf.role = Candidate
			rf.votedFor = rf.Me
			rf.votedNum = 1
			rf.currentTerm += 1
			rf.persist()
			//DPrintf("Raft[%d, %d] 开始选举...\n", rf.me, rf.currentTerm)
			rf.sendElection()
			rf.voteTimer = time.Now()
		}

		rf.mu.Unlock()

	}
}

// 心跳发送和日志同步
func (rf *Raft) appendTicker() {
	for rf.killed() == false {
		time.Sleep(HeartbeatSleep * time.Millisecond)
		rf.mu.Lock()
		if rf.role == Leader {
			//DPrintf("Raft[%d]是领导者...", rf.me)
			rf.mu.Unlock()
			go rf.leaderAppendEntries()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) committedTicker() {
	for rf.killed() == false {
		time.Sleep(AppliedSleep * time.Millisecond)
		rf.mu.Lock()

		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		Messages := make([]ApplyMsg, 0)
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() {
			rf.lastApplied += 1 //先更新lastApplied再提交，因为第一个空日志不用提交

			//rf.DApplyPrint(rf.lastApplied, rf.restoreLogTerm(rf.lastApplied), rf.restoreLog(rf.lastApplied).Command)
			Messages = append(Messages, ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				CommandIndex:  rf.lastApplied,
				Command:       rf.restoreLog(rf.lastApplied).Command,
				Term:          rf.restoreLog(rf.lastApplied).Term,
			})
		}

		rf.DPrintLogsLen()
		rf.mu.Unlock()
		for _, msg := range Messages {
			rf.applyChan <- msg
		}
	}
}

// -------------------------------------------Leader选举部分----------------------------------------------------------------

func (rf *Raft) sendElection() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.Me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.Me,
				LastLogIndex: rf.getLastIndex(),
				LastLogTerm:  rf.getLastTerm(),
			}
			reply := RequestVoteReply{}
			rf.mu.Unlock()

			res := rf.sendRequestVote(server, &args, &reply)

			if res == true {
				rf.mu.Lock()
				//DPrintf("Raft[%d, %d]要求投票返回true...", rf.me, rf.currentTerm)
				// 判断自己是否还是竞选者，且任期不冲突(其他候选者可能要求他投票, 然后修改他的状态)
				if rf.role != Candidate || args.Term < rf.currentTerm {
					rf.mu.Unlock()
					return
				}
				// 返回任期大于args（网络分区）、
				if reply.Term > args.Term { //发现了任期更大的节点，与其同步
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
					}
					rf.role = Follower
					rf.votedFor = -1 //任期内投票作废
					rf.votedNum = 0
					rf.voteTimer = time.Now() //
					rf.persist()
					rf.mu.Unlock()
					return
				}

				if reply.VoteGranted == true && rf.currentTerm == reply.Term {
					rf.votedNum++
					//DPrintf("Raft[%d]拿到%d个选票...", rf.me, rf.votedNum)
					if rf.votedNum > len(rf.peers)/2 {
						//DPrintf("Raft[%d, %d] 成为领导者...", rf.me, rf.currentTerm)
						rf.role = Leader
						rf.votedFor = -1
						rf.votedNum = 0
						rf.persist()

						rf.nextIndex = make([]int, len(rf.peers))
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = rf.getLastIndex() + 1
						}

						rf.matchIndex = make([]int, len(rf.peers))
						rf.matchIndex[rf.Me] = rf.getLastIndex()

						rf.voteTimer = time.Now()
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				return

			}
		}(i)
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//出现分区或者节点crash，导致候选者任期比接收者小，直接返回
	if args.Term <= rf.currentTerm {
		//fmt.Printf("在任期 [%v] 的Raft要求在任期 [%v] 的Raft投票，但是这个请求是过期的。不予投票\n", args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {
		//重置状态
		rf.role = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1 //发现更大的任期，任期内投票作废
		rf.votedNum = 0
		rf.persist()
	}

	if !rf.UpToDate(args.LastLogIndex, args.LastLogTerm) ||
		rf.votedFor != -1 && rf.votedFor != args.CandidateId && args.Term == rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else {
		//DPrintf("Raft[%d]投票给Raft[%d]...", rf.me, args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term //一旦投票，表示认可候选者，任期是一样的
		reply.Term = rf.currentTerm
		rf.voteTimer = time.Now()
		rf.persist()
		return
	}

}

// ---------------------------------------------- 日志增量部分 -----------------------------------------------------------

func (rf *Raft) leaderAppendEntries() {
	for index := range rf.peers {
		if index == rf.Me { //rf.me是只读变量，不用加锁
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			if rf.role != Leader {
				rf.mu.Unlock()
				return
			}

			//installSnapshot，如果rf.nextIndex[i]-1小于等lastIncludeIndex,说明followers的日志小于自身的快照状态，将自己的快照发过去
			// 同时要注意的是比快照还小时，已经算是比较落后

			if rf.nextIndex[server]-1 < rf.lastIncludeIndex {
				go rf.leaderSendSnapShot(server)
				rf.mu.Unlock()
				return
			}

			prevLogIndex, prevLogTerm := rf.getPrevLogInfo(server)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeadId:       rf.Me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: rf.commitIndex,
			}

			if rf.getLastIndex() >= rf.nextIndex[server] { //主节点有新日志可以更新到从节点
				entries := make([]LogEntry, 0)
				entries = append(entries, rf.logs[rf.nextIndex[server]-rf.lastIncludeIndex:]...)
				args.Entries = entries
			} else {
				args.Entries = []LogEntry{}
			}
			reply := AppendEntriesReply{}
			rf.mu.Unlock()

			re := rf.sendAppendEntries(server, &args, &reply)

			if re == true {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.role != Leader {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = Follower
					rf.votedFor = -1
					rf.votedNum = 0
					rf.persist()
					rf.voteTimer = time.Now()
					return
				}

				if reply.Success {
					rf.commitIndex = rf.lastIncludeIndex
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1

					for index := rf.getLastIndex(); index > rf.lastIncludeIndex; index-- {
						sum := 0
						for i := 0; i < len(rf.peers); i++ {
							if i == rf.Me {
								sum += 1
								continue
							}
							if rf.matchIndex[i] >= index {
								sum += 1
							}
						}

						if sum > len(rf.peers)/2 && rf.restoreLogTerm(index) == rf.currentTerm {
							rf.commitIndex = index
							break
						}
					}

				} else {
					if reply.UpNextIndex != -1 {
						rf.nextIndex[server] = reply.UpNextIndex
					}
				}
			}

		}(index)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = -1
		return
	}

	reply.Success = true
	reply.Term = args.Term
	reply.UpNextIndex = -1

	rf.role = Follower
	rf.currentTerm = args.Term
	rf.votedFor = -1 //已经有明确的领导者，不处于选举阶段
	rf.votedNum = 0
	rf.persist()
	rf.voteTimer = time.Now()

	if rf.lastIncludeIndex > args.PrevLogIndex {
		reply.Success = false
		reply.UpNextIndex = rf.getLastIndex() + 1
		return
	}

	//当前节点的最后一个日志的下标小于prev，说明日志有缺失
	if rf.getLastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.UpNextIndex = rf.getLastIndex() + 1
		return
	} else {
		if rf.restoreLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
			reply.Success = false
			tempTerm := rf.restoreLogTerm(args.PrevLogIndex)
			//回退到上一个任期的日志
			for index := args.PrevLogIndex; index >= rf.lastIncludeIndex; index-- {
				if rf.restoreLogTerm(index) != tempTerm {
					reply.UpNextIndex = index + 1
					break
				}
			}
			return
		}
	}

	if len(args.Entries) > 0 {
		rf.logs = append(rf.logs[:args.PrevLogIndex+1-rf.lastIncludeIndex], args.Entries...)
		//rf.DPrintLogs()
		rf.persist()

	}
	// commitIndex取leaderCommit与last new entry最小值的原因是，虽然应该更新到leaderCommit，但是new entry的下标更小
	// 则说明日志不存在，更新commit的目的是为了applied log，这样会导致日志日志下标溢出

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
	}
	return

}

// ------------------------------------------- 日志快照部分 --------------------------------------------------------------

func (rf *Raft) leaderSendSnapShot(server int) {
	rf.mu.Lock()

	args := InstallSnapShotArgs{
		Term:             rf.currentTerm,
		LeaderId:         rf.Me,
		LastIncludeIndex: rf.lastIncludeIndex,
		LastIncludeTerm:  rf.lastIncludeTerm,
		Data:             rf.persister.ReadSnapshot(),
	}

	reply := InstallSnapShotReply{}

	rf.mu.Unlock()

	re := rf.sendSnapShot(server, &args, &reply)

	if re == true {
		rf.mu.Lock()
		if rf.role != Leader || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		// 如果返回的Term大于自身Term，说明自身数据已经不适合了
		if reply.Term > rf.currentTerm {
			rf.role = Follower
			rf.votedFor = -1
			rf.votedNum = 0
			rf.persist()
			rf.voteTimer = time.Now()
			rf.mu.Unlock()
			return
		}

		rf.matchIndex[server] = args.LastIncludeIndex
		rf.nextIndex[server] = args.LastIncludeIndex + 1

		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) InstallSnapShot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	reply.Term = args.Term

	rf.role = Follower
	rf.votedFor = -1
	rf.votedNum = 0
	rf.persist()
	rf.voteTimer = time.Now()

	if rf.lastIncludeIndex >= args.LastIncludeIndex {
		rf.mu.Unlock()
		return
	}

	// 将快照前的直接apply
	index := args.LastIncludeIndex
	tempLog := make([]LogEntry, 0)
	tempLog = append(tempLog, LogEntry{})

	// 将当前节点位于快照后的log记录
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		tempLog = append(tempLog, rf.restoreLog(i))
	}

	rf.lastIncludeIndex = args.LastIncludeIndex
	rf.lastIncludeTerm = args.LastIncludeTerm

	rf.logs = tempLog
	if index > rf.commitIndex {
		rf.commitIndex = index
	}

	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	DPrintf("Raft[%d, %d] 接收快照 %d \n", rf.Me, rf.currentTerm, args.LastIncludeIndex)
	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludeTerm,
		SnapshotIndex: rf.lastIncludeIndex,
	}

	rf.mu.Unlock()
	rf.applyChan <- msg

}

// index是快照应用的index， snapshot代表的是上层service传来的快照字节流，

func (rf *Raft) SnapShot(index int, snapshot []byte) { //任何节点都可以自主创建快照，只要快照的下标小于已提交下标即可
	if rf.killed() {
		return
	}
	//DPrintf("--------------------------------------执行快照--------------------------------------------------")

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果下标大于自身的提交，说明没被提交不能安装快照，如果自身快照点大于index说明不需要安装
	if rf.lastIncludeIndex >= index || index > rf.commitIndex {
		return
	}

	//更新快照日志
	sLogs := make([]LogEntry, 0)
	sLogs = append(sLogs, LogEntry{})

	for i := index + 1; i <= rf.getLastIndex(); i++ {
		sLogs = append(sLogs, rf.restoreLog(i))
	}

	//更新快照下标/term
	if index == rf.getLastIndex()+1 { // +1是因为前面有一个空日志
		rf.lastIncludeTerm = rf.getLastTerm()
	} else {
		rf.lastIncludeTerm = rf.restoreLogTerm(index)
	}

	rf.lastIncludeIndex = index
	rf.logs = sLogs

	// apply了快照就应该重置commitIndex、lastApplied
	if index > rf.commitIndex {
		rf.commitIndex = index
	}

	if index > rf.lastApplied {
		rf.lastApplied = index
	}

	// 持久化快照信息
	DPrintf("Raft[%d, %d]创建快照 %d\n", rf.Me, rf.currentTerm, index)
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)

}

// --------------------------------------- 日志持久化部分 ----------------------------------------------------------------
func (rf *Raft) persistData() []byte {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	data := w.Bytes()
	//fmt.Printf("RaftNode[%d] persist starts, currentTerm[%d] voteFor[%d] log[%v]\n", rf.me, rf.currentTerm, rf.votedFor, rf.logs)
	return data
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	data := rf.persistData()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any status?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var lastIncludeIndex int
	var lastIncludeTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil {
		fmt.Println("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm
	}

}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

/**
上层服务调用Raft协议接口，追加日志
*/

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() == true {
		return -1, -1, false
	}

	if rf.role != Leader {
		return -1, -1, false
	} else {
		index := rf.getLastIndex() + 1
		term := rf.currentTerm
		//DPrintf("主节点[%d, %d]添加日志\n", rf.me, rf.currentTerm)
		rf.logs = append(rf.logs, LogEntry{term, command})
		rf.DPrintLogs()

		rf.persist()
		return index, term, true
	}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)

}
