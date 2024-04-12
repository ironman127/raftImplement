package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import mathRand "math/rand"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.leaderId = mathRand.Intn(len(ck.servers))
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.seqId++
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}
	serverId := ck.leaderId
	for {
		reply := GetReply{}
		//DPrintf("Client[%v] Get操作：%v", ck.clientId, key)
		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)

		if ok {
			if reply.Err == ErrNoKey {
				ck.leaderId = serverId
				return ""
			} else if reply.Err == OK {
				DPrintf("Client[%v, %v] Get [%v, %v] Success...", ck.clientId, ck.seqId, key, reply.Value)
				ck.leaderId = serverId
				return reply.Value
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}
		// 节点发生崩溃
		serverId = (serverId + 1) % len(ck.servers)
	}

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seqId++
	serverId := ck.leaderId
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}

	for {
		reply := PutAppendReply{}
		//DPrintf("Client[%v] %v操作：%v, %v", ck.clientId, op, key, value)
		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err == OK {
				DPrintf("Client[%v, %v] %v [%v, %v] Success...", ck.clientId, ck.seqId, op, key, value)
				ck.leaderId = serverId
				return
			} else if reply.Err == ErrWrongLeader {
				//DPrintf("Client[%v] %v操作：%v, %v失败....", ck.clientId, op, key, value)
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
