package shardkv

import "6.824/shardctrler"

/**
 * @Author: ygzhang
 * @Date: 2023/12/11 14:48
 * @Func:
 **/

// =================后台监听的线程=====================//
//
// applier
//
//	@Description: 循环监听applyChan, 收到一个cmd, 就应用到状态机, 并通知client
//	@receiver kv
func (kv *ShardKV) applier() {
	kv.mu.RLock()
	me := kv.me
	megid := kv.gid
	kv.mu.RUnlock()
	// 只要kvserver未停止就一直监听
	for !kv.killed() {
		// 阻塞的从applyCh读取rf传递过来的消息
		// NOTE: applier收到的msg有两种: 一种是client的leader主动apply的, 另一种是follower收到leader同步给它的
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid == true {
			cmd := applyMsg.Command.(Command) // 通过类型断言, 将接口转为Op类型
			index := applyMsg.CommandIndex
			term := applyMsg.CommandTerm
			kv.mu.Lock()
			// NOTE: 要小心日志的rollback, 因为raft去applyCh是不能保证原子性的, chan里的日志可能是乱序的, 所以要有一个lastApplied来保证不会回滚
			// 如果收到的是一个过时的旧日志, 直接抛弃, 避免出现log rollback
			if index <= kv.lastApplied {
				DPrintf(DApply, "server-%d-%d received out of date command and drop it. [applyMsg=%v]", megid, me, applyMsg)
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = index
			kv.mu.Unlock()
			// 将这个已经commit掉的日志应用到数据库里
			var msg OperationReply
			// 根据command的类型要进行不同处理
			switch cmd.Cmd {
			case OPERATION:
				op := cmd.CmdArgs.(OperationArgs)
				msg = kv.applyOperation(op)
			case CONFIG:
				latestConfig := cmd.CmdArgs.(shardctrler.Config)
				msg = kv.applyConfig(latestConfig)
			case ADDSHARDS:
				reply := cmd.CmdArgs.(ShardsReply)
				msg = kv.applyAddShards(reply)
			case REMOVESHARDS:
				args := cmd.CmdArgs.(ShardsArgs)
				msg = kv.applyRemoveShards(args)
			case NOOP:
				msg = kv.applyNoop()
			}
			// 将返回值通过ch通知对应的client发送端
			// NOTE: 只有当前还是leader, 才需要通知对应的client
			// NOTE: 不只是当前还是leader, 必须这个日志也是这个term内的日志才行, 如果是顺带提交之前term的日志, 也是不能通知client的, 否则会阻塞
			if currentTerm, isLeader := kv.rf.GetState(); isLeader && currentTerm == term {
				kv.mu.RLock()
				// NOTE: 同样的问题, 在使用map之前, 先检查map中对象是否还存在
				// NOTE: 遇到的bug是, 如果发生了超时, 导致对应的chan已经关闭, map中对应项已经delete, 此时ch就是一个零值nil的chan, 而从一个零值nil chan里读会永久阻塞
				ch, ok := kv.notifyChan[index]
				kv.mu.RUnlock()
				if ok {
					ch <- msg
					DPrintf(DApply, "server-%d-%d send apply reply to notifyChan[%v], msg=%v",
						megid, me, index, msg)
				}
			}
			kv.mu.Lock()
			// 判断是否需要快照, 当需要快照, 且本地log超过maxraftstate的时候需要建立快照
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
				//对KVServer中的特有的结构进行快照
				snapshot := kv.makeSnapshot()
				kv.rf.Snapshot(index, snapshot)
				DPrintf(DApply, "server-%d-%d over maxraftstate call rf.Snapshot to install snapshot, index=%v",
					megid, me, index)
			}
			kv.mu.Unlock()
		} else if applyMsg.SnapshotValid == true {
			//follower收到了raft传来的建立快照的消息, 建立对应的快照
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
				kv.installSnapshot(applyMsg.Snapshot)
				DPrintf(DApply, "server-%d-%d install snapshot from leader, [msg=%v]",
					kv.gid, kv.me, applyMsg)
				// 从快照中更新kv后, 不要忘记更新kv.lastApplied
				kv.lastApplied = applyMsg.SnapshotIndex
			}
			kv.mu.Unlock()
		} else {
			DPrintf(DApply, "server-%d-%d received unknown message, [msg=%v]",
				megid, me, applyMsg)
		}
	}
}

//
// isShardsServing
//  @Description: 检查该shards是否可用
//  @receiver kv
//  @param shardId
//  @return bool
//
func (kv *ShardKV) isShardsServing(shardId int) bool {
	gid := kv.currentConfig.Shards[shardId]
	shard, ok := kv.db[shardId]
	if !ok {
		// 如果该shard不存在, 直接return
		return false
	} else {
		status := shard.GetStatus()
		// 必须是shard属于当前gid, 且状态为service或者gc, 才能对外提供服务
		return gid == kv.gid && (status == Service || status == GC)
	}
}

func (kv *ShardKV) applyOperation(op OperationArgs) OperationReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var reply OperationReply
	shardId := key2shard(op.Key)
	// 首先判断该shards是否属于该group
	if isOk := kv.isShardsServing(shardId); !isOk {
		DPrintf(DApply, "server-%d-%d failed to process this shards. [op=%v]", kv.gid, kv.me, op)
		reply.Status, reply.Value = ErrWrongGroup, ""
		return reply
	}
	if isOutDated := kv.isOutDatedCommand(op.Seq, op.ClientId); isOutDated {
		DPrintf(DApply, "server-%d-%d received out of dated command. [op=%v]", kv.gid, kv.me, op)
		reply.Status, reply.Value = ErrOutofDate, ""
		return reply
	}
	if isDuplicated, value := kv.isDuplicatedCommand(op.Seq, op.ClientId); op.Op != GET && isDuplicated {
		// 如果是重复的操作, 直接从最新的写操作里面拿到最新的写值即可
		DPrintf(DApply, "server-%d-%d received duplicate command. [op=%v]", kv.gid, kv.me, op)
		reply.Status, reply.Value = "", value
	} else {
		// 不重复的最新操作, 才需要应用到状态机
		DPrintf(DApply, "server-%d-%d applied cmd to database, op=%v", kv.gid, kv.me, op)
		kv.mu.Unlock()
		reply = kv.applyToDatabase(op, shardId)
		kv.mu.Lock()
		// 只有写操作才需要更新lastOperation, 因为读操作不会引起一致性问题
		if op.Op != GET {
			kv.lastOperations[op.ClientId] = Operation{
				Op:       op.Op,
				Key:      op.Key,
				Value:    op.Value,
				ClientId: op.ClientId,
				Seq:      op.Seq,
			}
		}
	}
	return reply
}

func (kv *ShardKV) applyConfig(latestConfig shardctrler.Config) OperationReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 保证一次只能更新下一个的config
	if latestConfig.Num != kv.currentConfig.Num+1 {
		DPrintf(DApply, "server-%d-%d applyConfig failed for mismatch config num. [latestConfig=%v, currentConfig=%v]",
			kv.gid, kv.me, latestConfig, kv.currentConfig)
		return OperationReply{Status: ErrOutofDate}
	}
	// 如果成功需要更新当前的config为latestConfig
	for i := 0; i < shardctrler.NShards; i++ {
		//遍历所有切片, 将属于自己group的shard但现在不属于的设为pull, 之后要从其他分片拉过来
		if kv.currentConfig.Shards[i] != kv.gid && latestConfig.Shards[i] == kv.gid {
			if gid := kv.currentConfig.Shards[i]; gid != shardctrler.InvalidGid {
				kv.db[i].SetStatus(Pull)
			}
		}
		//将现在属于自己, 但之后不属于自己的分片设置为push, 之后分给其他分片
		if kv.currentConfig.Shards[i] == kv.gid && latestConfig.Shards[i] != kv.gid {
			if gid := kv.currentConfig.Shards[i]; gid != shardctrler.InvalidGid {
				kv.db[i].SetStatus(Pulled)
			}
		}
	}
	kv.lastConfig = kv.currentConfig
	kv.currentConfig = latestConfig
	DPrintf(DApply, "server-%d-%d applyConfig success update the currentConfig. [lastConfig=%v, currentConfig=%v]",
		kv.gid, kv.me, kv.lastConfig, kv.currentConfig)
	return OperationReply{Status: OK}
}

func (kv *ShardKV) applyAddShards(reply ShardsReply) OperationReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 保证只能更新同一config内的shards, 抛弃过时的
	if reply.ConfigNum != kv.currentConfig.Num {
		DPrintf(DApply, "server-%d-%d: applyAddShards failed for mismatch config num. [reply.ConfigNum=%v, currentConfig.Num=%v]",
			kv.gid, kv.me, reply.ConfigNum, kv.currentConfig.Num)
		return OperationReply{Status: ErrOutofDate}
	}
	DPrintf(DApply, "server-%d-%d: applyAddShards success add shards. [reply=%v]", kv.gid, kv.me, reply)
	for shardId, shardData := range reply.Shards {
		shard := kv.db[shardId]
		//NOTE: 这里必须要检查shard是否还是pull状态, 因为可能已经被其他add过, 是收到的重复的
		if shard.GetStatus() == Pull {
			shard.SetKVdb(shardData)
			//leader pull shards结束, 通知对方可以回收掉无用的shards了
			shard.SetStatus(GC)
		} else {
			DPrintf(DApply, "server-%d-%d applyAddShards ignore it for gotDuplicatedReply", kv.gid, kv.me)
			break
		}
	}
	//NOTE: 这里必须要在shard rpc里增加lastOperation字段, 防止重复apply过期的数据
	////考虑这种情况, 先pull, 然后来了个落后的cmd, 会pull完成后这个cmd已经完成了, 这时候cmd再来就不能被apply, 所以也要连带pull lastOperation
	for clientId, operation := range reply.LastOperations {
		if lastOperation, ok := kv.lastOperations[clientId]; !ok || lastOperation.Seq < operation.Seq {
			kv.lastOperations[clientId] = operation
		}
	}
	DPrintf(DApply, "server-%d-%d applyAddShards success add shards to shardkvdb", kv.gid, kv.me)
	return OperationReply{Status: OK}
}

func (kv *ShardKV) applyRemoveShards(args ShardsArgs) OperationReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 保证只能更新同一config内的shards, 抛弃过时的
	if args.ConfigNum != kv.currentConfig.Num {
		DPrintf(DApply, "server-%d-%d: applyRemoveShards failed for mismatch config num. [reply.ConfigNum=%v, currentConfig.Num=%v]",
			kv.gid, kv.me, args.ConfigNum, kv.currentConfig.Num)
		return OperationReply{Status: ErrOutofDate}
	}
	for _, shardId := range args.ShardIds {
		shard := kv.db[shardId]
		if shard.GetStatus() == GC {
			// 如果是GC状态, 说明已经成功删除了远端raft组的数据, 可以将shard设置为默认状态
			shard.SetStatus(Service)
		} else if shard.GetStatus() == Pulled {
			// 如果是pulled状态, 说明是本raft组第一次删除shard的数据, 此时直接重置分片
			kv.db[shardId] = NewShard()
		} else {
			// 否则, 说明收到了重复的rpc, 此时已经是service状态
			DPrintf(DApply, "server-%d-%d: applyRemoveShards received duplicated reply and ignore it. [args=%v]", kv.gid, kv.me, args)
			break
		}
	}
	DPrintf(DApply, "server-%d-%d: applyRemoveShards success remove shards. [args=%v]", kv.gid, kv.me, args)
	return OperationReply{Status: OK}
}

func (kv *ShardKV) applyNoop() OperationReply {
	return OperationReply{Status: OK}
}

// applyToDatabase
//
//	@Description: 根据op的类型, 将操作执行到状态机里, 也就是在数据库里执行对应的操作
//	@receiver kv
//	@param op
//	@return CommandReply
func (kv *ShardKV) applyToDatabase(op OperationArgs, shardId int) OperationReply {
	var reply OperationReply
	switch op.Op {
	case GET:
		val, err := kv.db[shardId].Get(op.Key)
		reply.Status, reply.Value = err, val
	case PUT:
		kv.db[shardId].Put(op.Key, op.Value)
	case APPEND:
		kv.db[shardId].Append(op.Key, op.Value)
	default:
	}
	return reply
}
