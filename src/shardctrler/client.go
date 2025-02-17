package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
)
import "time"
import "crypto/rand"
import "math/big"
import mathRand "math/rand"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	seqId    int
	leaderId int
	clientId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	ck.leaderId = mathRand.Intn(len(ck.servers))

	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.seqId++
	args := QueryArgs{
		Num:      num,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}

	serverId := ck.leaderId
	// Your code here.
	for {
		reply := QueryReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Query", &args, &reply)

		if ok {
			if reply.Err == Ok {
				ck.leaderId = serverId
				return reply.Config
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.seqId++

	args := JoinArgs{
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
		Servers:  servers,
	}

	serverId := ck.leaderId
	// Your code here.
	for {
		reply := JoinReply{}

		ok := ck.servers[serverId].Call("ShardCtrler.Join", &args, &reply)

		if ok {
			if reply.Err == Ok {
				ck.leaderId = serverId
				return
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.seqId++
	args := LeaveArgs{
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
		GIDs:     gids,
	}

	serverId := ck.leaderId
	// Your code here.
	for {
		reply := LeaveReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Leave", &args, &reply)

		if ok {
			if reply.Err == Ok {
				ck.leaderId = serverId
				return
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {

	ck.seqId++
	args := MoveArgs{
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
		Shard:    shard,
		GID:      gid,
	}

	serverId := ck.leaderId
	// Your code here.
	for {
		reply := MoveReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Move", &args, &reply)

		if ok {
			if reply.Err == Ok {
				ck.leaderId = serverId
				return
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
