package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
)
import "crypto/rand"
import "math/big"

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

// sendCommand
//
//	@Description: 发送Command的函数, 根据op封装对应的args, 调用rpc, 返回结果reply
//	@receiver ck
//	@param key
//	@param value
//	@param op
//	@return CmdReply
func (ck *Clerk) sendCommand(args CmdArgs) CmdReply {
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
		ok := ck.servers[leaderId].Call("ShardCtrler.HandlerCommand", &args, &reply)

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

func (ck *Clerk) Query(num int) Config {
	args := CmdArgs{}
	// Your code here.
	args.Num = num
	args.Op = QUERY
	args.ClientId = ck.clientId
	args.Seq = ck.seq
	reply := ck.sendCommand(args)
	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := CmdArgs{}
	// Your code here.
	args.Servers = servers
	args.Op = JOIN
	args.ClientId = ck.clientId
	args.Seq = ck.seq
	_ = ck.sendCommand(args)
}

func (ck *Clerk) Leave(gids []int) {
	args := CmdArgs{}
	// Your code here.
	args.GIDs = gids
	args.Op = LEAVE
	args.ClientId = ck.clientId
	args.Seq = ck.seq
	_ = ck.sendCommand(args)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := CmdArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.Op = MOVE
	args.ClientId = ck.clientId
	args.Seq = ck.seq
	_ = ck.sendCommand(args)
}

//func (ck *Clerk) Query(num int) Config {
//	args := &QueryArgs{}
//	// Your code here.
//	args.Num = num
//	for {
//		// try each known server.
//		for _, srv := range ck.servers {
//			var reply QueryReply
//			ok := srv.Call("ShardCtrler.Query", args, &reply)
//			if ok && reply.WrongLeader == false {
//				return reply.Config
//			}
//		}
//		time.Sleep(100 * time.Millisecond)
//	}
//}
//
//func (ck *Clerk) Join(servers map[int][]string) {
//	args := &JoinArgs{}
//	// Your code here.
//	args.Servers = servers
//
//	for {
//		// try each known server.
//		for _, srv := range ck.servers {
//			var reply JoinReply
//			ok := srv.Call("ShardCtrler.Join", args, &reply)
//			if ok && reply.WrongLeader == false {
//				return
//			}
//		}
//		time.Sleep(100 * time.Millisecond)
//	}
//}
//
//func (ck *Clerk) Leave(gids []int) {
//	args := &LeaveArgs{}
//	// Your code here.
//	args.GIDs = gids
//
//	for {
//		// try each known server.
//		for _, srv := range ck.servers {
//			var reply LeaveReply
//			ok := srv.Call("ShardCtrler.Leave", args, &reply)
//			if ok && reply.WrongLeader == false {
//				return
//			}
//		}
//		time.Sleep(100 * time.Millisecond)
//	}
//}
//
//func (ck *Clerk) Move(shard int, gid int) {
//	args := &MoveArgs{}
//	// Your code here.
//	args.Shard = shard
//	args.GID = gid
//
//	for {
//		// try each known server.
//		for _, srv := range ck.servers {
//			var reply MoveReply
//			ok := srv.Call("ShardCtrler.Move", args, &reply)
//			if ok && reply.WrongLeader == false {
//				return
//			}
//		}
//		time.Sleep(100 * time.Millisecond)
//	}
//}
