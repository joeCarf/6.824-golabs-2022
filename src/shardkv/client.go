package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.824/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64         //client的id
	leaderId map[int]int64 //每个group对应的leader的id, gid->leaderid
	seq      int64         //命令的id
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.leaderId = make(map[int]int64) //gid->leaderId
	ck.seq = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//

////shared by Put and Append.
////You will have to modify this function.
//

func (ck *Clerk) Get(key string) string {
	args := OperationArgs{
		Op:       GET,
		Key:      key,
		Value:    "",
		ClientId: ck.clientId,
		Seq:      ck.seq,
	}
	reply := ck.sendOperation(args)
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	args := OperationArgs{
		Op:       PUT,
		Key:      key,
		Value:    value,
		ClientId: ck.clientId,
		Seq:      ck.seq,
	}
	ck.sendOperation(args)
}

func (ck *Clerk) Append(key string, value string) {
	args := OperationArgs{
		Op:       APPEND,
		Key:      key,
		Value:    value,
		ClientId: ck.clientId,
		Seq:      ck.seq,
	}
	ck.sendOperation(args)
}

func (ck *Clerk) sendOperation(request OperationArgs) OperationReply {
	request.ClientId, request.Seq = ck.clientId, ck.seq
	for {
		shard := key2shard(request.Key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			if _, ok = ck.leaderId[gid]; !ok {
				ck.leaderId[gid] = 0
			}
			oldLeaderId := ck.leaderId[gid]
			newLeaderId := oldLeaderId
			for {
				var response OperationReply
				ok := ck.make_end(servers[newLeaderId]).Call("ShardKV.HandlerOperation", &request, &response)
				if ok && (response.Status == OK || response.Status == ErrNoKey) {
					ck.seq++
					return response
				} else if ok && response.Status == ErrWrongGroup {
					break
				} else {
					newLeaderId = (newLeaderId + 1) % int64(len(servers))
					if newLeaderId == oldLeaderId {
						break
					}
					continue
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

//func (ck *Clerk) sendOperation(args OperationArgs) OperationReply {
//	// 一直重试请求直到成功
//	for {
//		shard := key2shard(args.Key)
//		gid := ck.config.Shards[shard]
//		if servers, ok := ck.config.Groups[gid]; ok {
//			// try each server for the shard.
//			if _, ok = ck.leaderId[gid]; !ok {
//				ck.leaderId[gid] = 0
//			}
//			// 用leaderid记录下当前尝试的组的leader, 并在每次成功的时候都更新
//			leaderId := ck.leaderId[gid]
//			for {
//				srv := ck.make_end(servers[leaderId])
//				var reply OperationReply
//				DPrintf(DOPeration, "C%d -> %v send Command rpc, args = %v", ck.clientId, servers[leaderId], args)
//				rpcOk := srv.Call("ShardKV.HandlerOperation", &args, &reply)
//
//				if !rpcOk {
//					// rpc通信失败, 需要换个leader重传, 这里用的是轮询的方式
//					DPrintf(DOPeration, "C%v <- %v receive failed rpc for [ok=%v, reply=%v], retry %v",
//						ck.clientId, servers[leaderId], rpcOk, reply, servers[(leaderId+1)%int64(len(servers))])
//					leaderId = (leaderId + 1) % int64(len(servers))
//					//如果已经轮了一圈, 还没找到leader, 可能是因为这个group的正在更新配置, 或者选举leader, 重新发起一下
//					if leaderId == ck.leaderId[gid] {
//						DPrintf(DOPeration, "C%d try all servers in group, maybe config changing, need refresh config", ck.clientId)
//						break
//					}
//					continue
//				} else {
//					// NOTE: 不能用switch+break, 因为break默认是跳出for或者switch结构
//					if reply.Status == ErrWrongLeader || reply.Status == ErrTimeout {
//						// 如果失败, 或是超时和错误的leader, 都换leader重传
//						DPrintf(DOPeration, "C%v <- %v receive failed result, [ok=%v, reply=%v], retry %v",
//							ck.clientId, servers[leaderId], ok, reply, servers[(leaderId+1)%int64(len(servers))])
//						leaderId = (leaderId + 1) % int64(len(servers))
//						//如果已经轮了一圈, 还没找到leader, 可能是因为这个group的正在更新配置, 或者选举leader, 重新发起一下
//						if leaderId == ck.leaderId[gid] {
//							DPrintf(DOPeration, "C%d try all servers in group, maybe config changing, need refresh config", ck.clientId)
//							break
//						}
//						continue
//					} else if reply.Status == ErrWrongGroup {
//						// 如果查询到错误的组, 需要更新配置, 然后再查询
//						DPrintf(DOPeration, "C%v <- %v receive failed result, Wrong Group!", ck.clientId, servers[leaderId])
//						break
//					} else if reply.Status == ErrNoKey {
//						// 如果查询结果失败, 则返回空
//						DPrintf(DOPeration, "C%v <- %v receive empty result, No key!", ck.clientId, servers[leaderId])
//						ck.leaderId[gid] = leaderId
//						ck.seq++ //seq自增
//						return reply
//					} else {
//						// 默认都是OK的, 直接返回结果就行
//						DPrintf(DOPeration, "C%v <- %v receive successful result. [args=%v, reply=%v]",
//							ck.clientId, servers[leaderId], args, reply)
//						ck.leaderId[gid] = leaderId
//						ck.seq++ //seq自增
//						return reply
//					}
//				}
//			}
//		}
//		time.Sleep(100 * time.Millisecond)
//		// ask controler for the latest configuration.
//		ck.config = ck.sm.Query(-1)
//		DPrintf(DOPeration, "C%v refresh the config.", ck.clientId)
//	}
//}
