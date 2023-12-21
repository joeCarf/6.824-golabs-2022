package shardkv

import (
	"6.824/shardctrler"
	"sync"
	"time"
)

/**
 * @Author: ygzhang
 * @Date: 2023/12/14 18:44
 * @Func:
 **/

const (
	ConfigurationTimeout = 200 * time.Millisecond
	PullShardTimeout     = 50 * time.Millisecond
	GCShardTimeout       = 50 * time.Millisecond
	NoopTimeout          = 200 * time.Millisecond
)

//=========ticker中包含的四种操作: configuration
//=========ticker触发的协程, 都要求必须是leader才能执行================//

//
// tickerConfiguration
//  @Description: 周期性拉取最新配置
//  @receiver kv
//
func (kv *ShardKV) tickerConfiguration() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.RLock()
			// 只有所有shard都是service状态, 才可以拉取配置, 否则说明现在当前group正处在其他状态中pull,push,gc
			isOK := true
			for shardId, shard := range kv.db {
				status := shard.GetStatus()
				if status == Pull || status == Pulled || status == GC {
					isOK = false
					DPrintf(DConfig, "server-%d-%d: tickerConfiguration pull failed for can not pull config![shardId=%v, status=%v]",
						kv.gid, kv.me, shardId, status)
					break
				}
			}
			currentConfig := kv.currentConfig
			kv.mu.RUnlock()
			if isOK {
				// FIXME:为什么不拉取最新配置
				latestConfig := kv.sc.Query(currentConfig.Num + 1)
				if latestConfig.Num == currentConfig.Num+1 {
					DPrintf(DConfig, "server-%d-%d: tickerConfiguration pull latest config. [latestConfig=%v, currentConfig=%v]",
						kv.gid, kv.me, latestConfig, currentConfig)
					cmd := Command{
						Cmd:     CONFIG,
						CmdArgs: latestConfig,
					}
					// 拉取最新配置也要通过raft层共识, 保证follower也能拉取最新配置
					kv.startCommandToRaft(cmd, &OperationReply{})
				}
			}
		}
		time.Sleep(ConfigurationTimeout)
	}
}

//
// tickerPullShard
//  @Description: 周期性监测, 是否有pull状态的shard
//  @receiver kv
//
func (kv *ShardKV) tickerPullShard() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.RLock()
			g2sMap := kv.getg2sMapByStatus(Pull)
			// NOTE: 使用wg而不是循环发送, 是为了减少重复pull rpc的次数
			var wg sync.WaitGroup
			me := kv.me
			mGid := kv.gid
			currentConfig := kv.currentConfig
			lastConfig := kv.lastConfig
			kv.mu.RUnlock()

			//根据g2sMap的记录, 向对应gid发送rpc请求pull对应的shard
			for gid, shardIds := range g2sMap {
				wg.Add(1)
				go func(gid int, shardIds []int) {
					defer wg.Done()
					args := ShardsArgs{
						ShardIds:  shardIds,
						ConfigNum: currentConfig.Num,
					}
					for _, server := range lastConfig.Groups[gid] {
						srv := kv.make_end(server)
						reply := ShardsReply{}
						DPrintf(DPull, "server-%d-%d -> R%v send pull rpc. [args=%v]", mGid, me, server, args)
						ok := srv.Call("ShardKV.HandlerPullShards", &args, &reply)
						// 如果rpc失败了, 或者返回值status不为OK, 说明rpc失败了
						if !ok || reply.Status != OK {
							DPrintf(DPull, "server-%d-%d <- R%v pull rpc failed. [ok=%v, reply=%v]", mGid, me, server, ok, reply)
						} else {
							// 说明rpc成功了, 调用raft达成共识
							DPrintf(DPull, "server-%d-%d <- R%v pull rpc successed, start cmd in raft. [ok=%v, reply=%v]",
								mGid, me, server, ok, reply)
							cmd := Command{
								Cmd:     ADDSHARDS,
								CmdArgs: reply,
							}
							kv.startCommandToRaft(cmd, &OperationReply{})
							return
						}
					}
				}(gid, shardIds)
			}
			wg.Wait()
		}
		time.Sleep(PullShardTimeout)
	}
}

//
// tickerGarbageCollection
//  @Description: 周期性监测是否有需要gc的shards
//  @receiver kv
//
func (kv *ShardKV) tickerGarbageCollection() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.RLock()
			g2sMap := kv.getg2sMapByStatus(GC)
			// 同样是用wait group来避免重复发送, 减少重复rpc
			var wg sync.WaitGroup
			me := kv.me
			mGid := kv.gid
			currentConfig := kv.currentConfig
			lastConfig := kv.lastConfig
			kv.mu.RUnlock()

			//根据g2sMap的记录, 向对应gid发送rpc请求pull对应的shard
			for gid, shardIds := range g2sMap {
				wg.Add(1)
				go func(gid int, shardIds []int) {
					defer wg.Done()
					args := ShardsArgs{
						ShardIds:  shardIds,
						ConfigNum: currentConfig.Num,
					}
					for _, server := range lastConfig.Groups[gid] {
						srv := kv.make_end(server)
						reply := ShardsReply{}
						DPrintf(DRemove, "server-%d-%d -> R%v send remove rpc. [args=%v]", mGid, me, server, args)
						ok := srv.Call("ShardKV.HandlerRemoveShards", &args, &reply)
						// 如果rpc失败了, 或者返回值status不为OK, 说明rpc失败了
						if !ok || reply.Status != OK {
							DPrintf(DRemove, "server-%d-%d <- R%v remove rpc failed. [ok=%v, reply=%v]",
								mGid, me, server, ok, reply)
						} else {
							// 说明rpc成功了, 调用raft达成共识
							DPrintf(DRemove, "server-%d-%d <- R%v remove rpc successed, start cmd in raft. [ok=%v, reply=%v]",
								mGid, me, server, ok, reply)
							cmd := Command{
								Cmd:     REMOVESHARDS,
								CmdArgs: args,
							}
							kv.startCommandToRaft(cmd, &OperationReply{})
							return
						}
					}
				}(gid, shardIds)
			}
			wg.Wait()
		}
		time.Sleep(GCShardTimeout)
	}
}

func (kv *ShardKV) tickerNoop() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			if !kv.rf.GetIsLogInCurrentTerm() {
				kv.mu.RLock()
				me := kv.me
				mGid := kv.gid
				kv.mu.RUnlock()
				DPrintf(DNoop, "server-%d-%d ticker a noop, start a command to raft!", mGid, me)
				cmd := Command{
					Cmd:     NOOP,
					CmdArgs: nil,
				}
				kv.startCommandToRaft(cmd, &OperationReply{})
			}
		}
		time.Sleep(NoopTimeout)
	}
}

//=============内部用的一些功能函数===============//
//
// getg2sMapbyStatus
//  @Description: 遍历shard db, 找到所有shard.status=status的shardId, 并将其按照gid分组返回
//  @receiver kv
//  @param status
//  @return map[int][]int	gid->shards[]
//
func (kv *ShardKV) getg2sMapByStatus(status ShardStatus) map[int][]int {
	g2sMap := make(map[int][]int)
	for shardId, shard := range kv.db {
		if shard.GetStatus() == status {
			// 确保gid是有效的gid
			if gid := kv.lastConfig.Shards[shardId]; gid != shardctrler.InvalidGid {
				// 如果是空的要新建一个
				if _, ok := g2sMap[gid]; !ok {
					g2sMap[gid] = make([]int, 0)
				}
				g2sMap[gid] = append(g2sMap[gid], shardId)
			}
		}
	}
	return g2sMap
}
