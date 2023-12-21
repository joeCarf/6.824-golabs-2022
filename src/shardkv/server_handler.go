package shardkv

import "time"

/**
 * @Author: ygzhang
 * @Date: 2023/12/14 19:09
 * @Func:
 **/
//
// HandlerOperation
//  @Description: Operation RPC的处理函数, 将一个Op Cmd提交到raft并执行到kvdb中
//  @receiver kv
//  @param args
//  @param reply
//
func (kv *ShardKV) HandlerOperation(args *OperationArgs, reply *OperationReply) {
	kv.mu.RLock()
	me := kv.me
	gid := kv.gid
	// rpc去重, 如果收到重复的写rpc, 直接返回而不对db进行操作
	// NOTE: 必须用ok判断map是否存在, 否则直接调用会默认创建一个新的
	//if args.Op != GET && args.Seq == kv.lastOperations[args.ClientId].Seq {
	if isDuplicated, value := kv.isDuplicatedCommand(args.Seq, args.ClientId); args.Op != GET && isDuplicated {
		//收到了重复的RPC, 直接返回
		reply.Status, reply.Value = OK, value
		DPrintf(DOPeration, "server-%d-%d -> C%v received duplicate operation, reply to client. [args=%v, reply=%v]",
			gid, me, args.ClientId, args, reply)
		kv.mu.RUnlock()
		return
	}
	// 判断当前是否提供查询服务
	if !kv.isShardsServing(key2shard(args.Key)) {
		reply.Status = ErrWrongGroup
		DPrintf(DOPeration, "server-%d-%d -> C%v refuse to process operation for shard not serve, reply to client. [args=%v, reply=%v], [db=%v,currentConfig=%v]",
			gid, me, args.ClientId, args, reply, kv.db, kv.currentConfig)
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	// 包装一个command, 并调用raft.start来执行这个cmd
	cmd := Command{
		Cmd:     OPERATION,
		CmdArgs: *args,
	}
	// 交给raft层去共识, 并处理返回结果
	kv.startCommandToRaft(cmd, reply)
	DPrintf(DOPeration, "server-%d-%d -> C%v reply to client. [reply=%v]",
		gid, me, args.ClientId, reply)
}

//
// HandlerPullShards
//  @Description: Pull Shards RPC的处理函数, 需要将复制给发送方的shard data放到reply里
//  @receiver kv
//  @param args
//  @param reply
//
func (kv *ShardKV) HandlerPullShards(args *ShardsArgs, reply *ShardsReply) {
	// 如果不是leader 直接返回WrongLeader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Status = ErrWrongLeader
		return
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	DPrintf(DPull, "server-%d-%d HandlerPullShards received pull shards rpc [args=%v]",
		kv.gid, kv.me, args)
	// 如果请求拉取的是未来的数据, 返回错误让其稍后重试
	if args.ConfigNum > kv.currentConfig.Num {
		// 如果收到了更新的config num, 说明自己当前不够新, 不支持pull shard
		reply.Status = ErrOutofDate
		DPrintf(DPull, "server-%d-%d HandlerPullShards received out of date rpc [args=%v, reply=%v]",
			kv.gid, kv.me, args, reply)
		return
	}
	// 复制切片到args.Shards中
	reply.Shards = make(map[int]KVdb)
	for _, shardId := range args.ShardIds {
		// 这里需要复制当前切片, 并且是deep copy
		reply.Shards[shardId] = kv.db[shardId].GetKVdb()
	}

	//拷贝lastOperation到args.LastOperation
	reply.LastOperations = make(map[int64]Operation)
	for clientId, operation := range kv.lastOperations {
		reply.LastOperations[clientId] = operation
	}

	reply.ConfigNum, reply.Status = args.ConfigNum, OK
	return
}

//
// HandlerRemoveShards
//  @Description: remove shards RPC的处理函数, 将shards data删掉并gc
//  @receiver kv
//  @param args
//  @param reply
//
func (kv *ShardKV) HandlerRemoveShards(args *ShardsArgs, reply *ShardsReply) {
	// 如果不是leader 直接返回WrongLeader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Status = ErrWrongLeader
		return
	}
	kv.mu.RLock()
	DPrintf(DRemove, "server-%d-%d HandlerRemoveShards received remove shards rpc [args=%v]",
		kv.gid, kv.me, args)
	if args.ConfigNum < kv.currentConfig.Num {
		// 如果收到了更新的config num, 说明已经删掉了, 返回OK即可
		reply.Status = OK
		DPrintf(DRemove, "server-%d-%d HandlerRemoveShards received out of date rpc [args=%v, reply=%v]",
			kv.gid, kv.me, args, reply)
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	// 通过raft层达成共识, 将对应的shards删掉, 包装一个command, 并调用raft.start来执行这个cmd
	cmd := Command{
		Cmd:     REMOVESHARDS,
		CmdArgs: *args,
	}
	cmdReply := OperationReply{}
	// 交给raft层去共识, 并处理返回结果
	kv.startCommandToRaft(cmd, &cmdReply)
	reply.Status = cmdReply.Status
}

func (kv *ShardKV) startCommandToRaft(cmd Command, reply *OperationReply) {
	// NOTE: 日志提交的时候不需要持锁, 因为是交给raft层的Start()实现的, raft层已经保证了互斥, 这里没必要阻塞
	index, _, isLeader := kv.rf.Start(cmd)
	kv.mu.RLock()
	me := kv.me
	gid := kv.gid
	kv.mu.RUnlock()
	// 如果不是leader, 返回ErrWrongLeader
	if !isLeader {
		DPrintf(DCommand, "server-%d-%d refuse for not leader!", gid, me)
		reply.Status, reply.Value = ErrWrongLeader, ""
		return
	}
	DPrintf(DCommand, "server-%d-%d leader start a command. [index=%v, cmd=%v]", gid, me, index, cmd)
	// 创建一个index对应的chan用来接受这个index的返回值
	kv.mu.Lock()
	kv.notifyChan[index] = make(chan OperationReply)
	ch := kv.notifyChan[index]
	kv.mu.Unlock()
	// NOTE: 这里必须得做一个超时处理, 防止client协程一直阻塞;
	// 阻塞等待返回值或者执行时间超时
	select {
	case msg := <-ch:
		reply.Status, reply.Value = msg.Status, msg.Value
		DPrintf(DCommand, "server-%d-%d finish command[%v] in raft success, and reply=%v", gid, me, index, reply)
	case <-time.After(ExecutionTimeout):
		reply.Status, reply.Value = ErrTimeout, ""
		DPrintf(DCommand, "server-%d-%d execute command[%v] in raft time out!", gid, me, index)
	}
	// 不管是正常结束还是超时, 最后要释放掉这个chan
	//NOTE: 这里可以另起一个协程来释放, 可以增加吞吐量, 没必要阻塞在这里
	go func(index int) {
		kv.mu.Lock()
		delete(kv.notifyChan, index)
		kv.mu.Unlock()
	}(index)
}
