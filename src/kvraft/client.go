package kvraft

import (
	"6.824/labrpc"
	"crypto/rand"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	//mu       sync.RWMutex //用来互斥访问的mutex
	clientId int64 //client的id
	leaderId int64 //leader的id
	seq      int64 //命令的id
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
	ck.leaderId = 0
	ck.seq = 0
	return ck
}

// NOTE:把Get和Put, Append都抽象成一个Command呢, 感觉区别也就是有无返回值的Value,
//
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
	reply := ck.sendCommand(key, "", "Get")
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//func (ck *Clerk) PutAppend(key string, value string, op string) {
//	// You will have to modify this function.
//}

func (ck *Clerk) Put(key string, value string) {
	//ck.PutAppend(key, value, "Put")
	ck.sendCommand(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	//ck.PutAppend(key, value, "Append")
	ck.sendCommand(key, value, "Append")
}

// sendCommand
//
//	@Description: 发送Command的函数, 根据op封装对应的args, 调用rpc, 返回结果reply
//	@receiver ck
//	@param key
//	@param value
//	@param op
//	@return CmdReply
func (ck *Clerk) sendCommand(key string, value string, op string) CmdReply {
	var args CmdArgs
	switch op {
	case "Get":
		args = CmdArgs{
			Op:       GET,
			Key:      key,
			Value:    "",
			ClientId: ck.clientId,
			Seq:      ck.seq,
		}
	case "Put":
		args = CmdArgs{
			Op:       PUT,
			Key:      key,
			Value:    value,
			ClientId: ck.clientId,
			Seq:      ck.seq,
		}
	case "Append":
		args = CmdArgs{
			Op:       APPEND,
			Key:      key,
			Value:    value,
			ClientId: ck.clientId,
			Seq:      ck.seq,
		}
	}
	// 用leaderid记录下当前尝试的leader, 并在每次成功的时候都更新
	leaderId := ck.leaderId
	// 退出的时候, 肯定是找到leader并执行了rpc, 因此要更新对应的结构
	defer func() {
		ck.leaderId = leaderId
		ck.seq++ //seq自增
	}()
	for {
		// 一直重试请求直到成功
		reply := CmdReply{}
		DPrintf(dCommand, "C%d -> R%d send Command rpc, args = %v", ck.clientId, leaderId, args)
		ok := ck.servers[leaderId].Call("KVServer.HandlerCommand", &args, &reply)

		if !ok {
			// rpc通信失败, 需要换个leader重传, 这里用的是轮询的方式
			DPrintf(dCommand, "C%v <- R%d receive failed rpc for [ok=%v, reply=%v], retry R%d",
				ck.clientId, leaderId, ok, reply, (leaderId+1)%int64(len(ck.servers)))
			leaderId = (leaderId + 1) % int64(len(ck.servers))
			continue
		} else {
			switch reply.Status {
			case ErrWrongLeader, ErrTimeout:
				// 如果失败, 或是超时和错误的leader, 都换leader重传
				DPrintf(dCommand, "C%v <- R%d receive failed result, [ok=%v, reply=%v], retry R%d",
					ck.clientId, leaderId, ok, reply, (leaderId+1)%int64(len(ck.servers)))
				leaderId = (leaderId + 1) % int64(len(ck.servers))
				continue
			case ErrNoKey:
				// 如果查询结果失败, 则返回空
				DPrintf(dCommand, "C%v <- R%d receive empty result, No key!", ck.clientId, leaderId)
				return reply
			default:
				// 默认都是OK的, 直接返回结果就行
				DPrintf(dCommand, "C%v <- R%d receive successful result. [args=%v, reply=%v]", ck.clientId, leaderId, args, reply)
				return reply
			}
		}
	}
}
