package raft

import (
	"log"
	"math/rand"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) DPrintLogs() (n int, err error) {
	if Debug {
		cnt := 0
		if rf.role == Leader {
			log.Printf("主节点Raft[%d]日志: ", rf.Me)
		} else {
			log.Printf("从节点Raft[%d]日志: ", rf.Me)
		}
		for _, entry := range rf.logs {
			log.Printf("[%d, %d, %v]", rf.lastIncludeIndex+cnt, entry.Term, entry.Command)
			cnt++
		}
	}
	return
}

func (rf *Raft) DPrintLogsLen() (n int, err error) {
	if Debug {
		if rf.role == Leader {
			log.Printf("主节点Raft[%d]已提交日志下标: ", rf.Me)
		} else {
			log.Printf("从节点Raft[%d]已提交日志下标: ", rf.Me)
		}
		log.Printf("[1 - %v]", rf.lastApplied)
	}
	return
}

func (rf *Raft) DApplyPrint(index int, term int, m interface{}) (n int, err error) {
	if Debug {
		if rf.role == Leader {
			log.Printf("主节点Raft[%d]提交日志: ", rf.Me)
		} else {
			log.Printf("从节点Raft[%d]提交日志: ", rf.Me)
		}
		log.Printf("[%d, %d, %d]", index, term, m)
	}
	return
}

func min(num int, num1 int) int {
	if num > num1 {
		return num1
	} else {
		return num
	}
}

// 通过不同的随机种子生成不同的过期时间
func generateOverTime(server int64) int {
	rand.Seed(time.Now().Unix() + server)
	return rand.Intn(MoreVoteTime) + MinVoteTime
}

// 获取最后日志的下标
func (rf *Raft) getLastIndex() int {
	return len(rf.logs) - 1 + rf.lastIncludeIndex
}

func (rf *Raft) getLastTerm() int {
	if len(rf.logs) == 1 {
		return rf.lastIncludeTerm
	} else {
		return rf.logs[len(rf.logs)-1].Term
	}
}

// rpc调用
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendSnapShot(server int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}

// UpToDate 判断候选者的日志是否最新
func (rf *Raft) UpToDate(index int, term int) bool {
	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}

// 通过快照还原真实的日志任期
func (rf *Raft) restoreLogTerm(curIndex int) int {
	//如果当前index与快照包含的最后的index一直，则日志为空，直接返回快照中最后的任期

	if curIndex == rf.lastIncludeIndex {
		return rf.lastIncludeTerm
	}
	return rf.logs[curIndex-rf.lastIncludeIndex].Term
}

// 通过快照偏移还原真实日志条目
func (rf *Raft) restoreLog(curIndex int) LogEntry {
	return rf.logs[curIndex-rf.lastIncludeIndex]
}

func (rf *Raft) getPrevLogInfo(server int) (int, int) {
	newEntryBeginIndex := rf.nextIndex[server] - 1
	lastIndex := rf.getLastIndex()
	if newEntryBeginIndex == lastIndex+1 {
		newEntryBeginIndex = lastIndex
	}
	return newEntryBeginIndex, rf.restoreLogTerm(newEntryBeginIndex)
}
