package shardctrler

import (
	"6.824/raft"
	"fmt"
	"github.com/sasha-s/go-deadlock"
	"time"
)
import "6.824/labrpc"
import "6.824/labgob"

type ShardCtrler struct {
	mu      deadlock.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	//configs []Config // indexed by config num
	// Your definitions here.
	lastApplied    int                  // 记录了raft中的lastApplied, 防止apply的顺序, 导致日志的回滚
	notifyChan     map[int]chan Message //index->chan的map, 用来通知client协程, applier已经响应了
	lastOperations map[int64]Operation  //clientId->Operation, 为每个client维护一个最后执行的operation的map, 方便去重
	db             *CfgDB
}

const ExecutionTimeout = 500 * time.Millisecond

type Op struct {
	// Your data here.
	Opt      OptType          //Opt的类型
	Servers  map[int][]string // for Join
	GIDs     []int            // for Leave
	Shard    int              // for Move
	GID      int              // for Move
	Num      int              // for Query
	Cfg      Config
	ClientId int64 //提交opt的client
	Seq      int64 //该opt的seq
}

//======将opt+seq封装为了一个新结构operation
type Operation Op

//func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
//	// Your code here.
//}
//
//func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
//	// Your code here.
//}
//
//func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
//	// Your code here.
//}
//
//func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
//	// Your code here.
//}

//============================内部需要用到的一些功能函数==============================//
//
// isDuplicatedCommand
//  @Description: 用来判断是否是重复的日志
//  @receiver kv
//  @param seq
//  @param clientId
//  @return bool
//  @return string
//
func (sc *ShardCtrler) isDuplicatedCommand(seq int64, clientId int64) (bool, Config) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	// 从map中找到client对应的最新apply的cmd, 如果该kv不存在，则返回false
	lastOperation, ok := sc.lastOperations[clientId]
	if ok {
		return seq == lastOperation.Seq, lastOperation.Cfg
	} else {
		return false, Config{}
	}
}
func (sc *ShardCtrler) isOutDatedCommand(seq int64, clientId int64) bool {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	lastOperation, ok := sc.lastOperations[clientId]
	if ok {
		return lastOperation.Seq > seq
	} else {
		return false
	}
}

func (sc *ShardCtrler) HandlerCommand(args *CmdArgs, reply *CmdReply) {
	sc.mu.RLock()
	me := sc.me
	sc.mu.RUnlock()
	// rpc去重, 如果收到重复的写rpc, 直接返回而不对db进行操作
	if isDuplicated, cfg := sc.isDuplicatedCommand(args.Seq, args.ClientId); args.Op != QUERY && isDuplicated {
		//收到了重复的RPC, 直接返回
		reply.Status, reply.Config = OK, cfg
		DPrintf(dCommand, "R%d -> C%v received duplicate command, return, [args=%v, reply=%v]",
			me, args.ClientId, args, reply)
		return
	}
	// 包装一个command, 并调用raft.start来执行这个cmd
	op := Op{
		Opt:      args.Op,
		Servers:  args.Servers,
		GIDs:     args.GIDs,
		Shard:    args.Shard,
		GID:      args.GID,
		Num:      args.Num,
		Cfg:      Config{},
		ClientId: args.ClientId,
		Seq:      args.Seq,
	}
	// 日志提交的时候不需要持锁
	index, _, isLeader := sc.rf.Start(op)
	// 如果不是leader, 返回ErrWrongLeader
	if !isLeader {
		DPrintf(dCommand, "R%d -> C%v refuse for not leader!", me, args.ClientId)
		reply.Status, reply.Config = ErrWrongLeader, Config{}
		return
	}
	DPrintf(dCommand, "R%d -> C%v leader start a command, op=%v", me, args.ClientId, op)
	// 创建一个index对应的chan用来接受这个index的返回值
	sc.mu.Lock()
	sc.notifyChan[index] = make(chan Message)
	ch := sc.notifyChan[index]
	sc.mu.Unlock()
	// 阻塞等待返回值或者执行时间超时
	select {
	case msg := <-ch:
		reply.Status, reply.Config = msg.Err, msg.Payload
		DPrintf(dCommand, "R%d -> C%v finish command success, and reply=%v", me, args.ClientId, reply)
	case <-time.After(ExecutionTimeout):
		reply.Status, reply.Config = ErrTimeout, Config{}
		DPrintf(dCommand, "R%d -> C%v execute command time out!", me, args.ClientId)
	}
	// 不管是正常结束还是超时, 最后要释放掉这个chan
	go func(index int) {
		sc.mu.Lock()
		delete(sc.notifyChan, index)
		sc.mu.Unlock()
	}(index)
}

//func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
//	// Your code here.
//}

//func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
//	// Your code here.
//}

// =================后台监听的线程=====================//
//
// applier
//
//	@Description: 循环监听applyChan, 收到一个cmd, 就应用到状态机, 并通知client
//	@receiver kv
func (sc *ShardCtrler) applier() {
	sc.mu.RLock()
	me := sc.me
	sc.mu.RUnlock()
	// 只要kvserver未停止就一直监听
	for {
		// 阻塞的从applyCh读取rf传递过来的消息
		// NOTE: applier收到的msg有两种: 一种是client的leader主动apply的, 另一种是follower收到leader同步给它的
		applyMsg := <-sc.applyCh
		if applyMsg.CommandValid == true {
			op := applyMsg.Command.(Op) // 通过类型断言, 将接口转为Op类型
			index := applyMsg.CommandIndex
			term := applyMsg.CommandTerm
			sc.mu.Lock()
			// 如果收到的是一个过时的旧日志, 直接抛弃, 避免出现log rollback
			if index <= sc.lastApplied {
				DPrintf(dApply, "R%d received out of date command and drop it. [applyMsg=%v]", me, applyMsg)
				sc.mu.Unlock()
				continue
			}
			sc.lastApplied = index
			sc.mu.Unlock()
			// 将这个已经commit掉的日志应用到数据库里
			// 在应用状态机之前, 务必检查是否是重复操作, 保持线性一致性
			var msg Message
			if isOutDated := sc.isOutDatedCommand(op.Seq, op.ClientId); isOutDated {
				DPrintf(dApply, "R%d received out of dated command. [op=%v]", me, op)
				msg.Payload, msg.Err = Config{}, ErrOutofDate
				return
			}
			if isDuplicated, value := sc.isDuplicatedCommand(op.Seq, op.ClientId); op.Opt != QUERY && isDuplicated {
				// 如果是重复的操作, 直接从最新的写操作里面拿到最新的写值即可
				DPrintf(dApply, "R%d received duplicate command. [op=%v]", me, op)
				msg.Payload, msg.Err = value, ""
			} else {
				// 不重复的最新操作, 才需要应用到状态机
				DPrintf(dApply, "R%d applied cmd to database, op=%v", me, op)
				msg = sc.applyToDatabase(op)
				sc.mu.Lock()
				// 只有写操作才需要更新lastOperation, 因为读操作不会引起一致性问题
				if op.Opt != QUERY {
					sc.lastOperations[op.ClientId] = Operation{
						Opt:      op.Opt,
						Servers:  op.Servers,
						GIDs:     op.GIDs,
						Shard:    op.Shard,
						GID:      op.GID,
						Num:      op.Num,
						Cfg:      op.Cfg,
						ClientId: op.ClientId,
						Seq:      op.Seq,
					}
				}
				sc.mu.Unlock()
			}
			// 将返回值通过ch通知对应的client发送端, 条件是必须是leader且term对应
			if currentTerm, isLeader := sc.rf.GetState(); isLeader && currentTerm == term {
				sc.mu.RLock()
				ch, ok := sc.notifyChan[index]
				sc.mu.RUnlock()
				if ok {
					ch <- msg
					DPrintf(dApply, "R%d send apply reply to client, op=%v", me, op)
				}
			}
		} else {
			DPrintf(dApply, "R%d received unknown message, [msg=%v]", me, applyMsg)
		}
	}
}
func (sc *ShardCtrler) applyToDatabase(op Op) Message {
	switch op.Opt {
	case JOIN:
		err := sc.db.Join(op.Servers)
		return Message{Err: err}
	case LEAVE:
		err := sc.db.Leave(op.GIDs)
		return Message{Err: err}
	case MOVE:
		err := sc.db.Move(op.Shard, op.GID)
		return Message{Err: err}
	case QUERY:
		cfg, err := sc.db.Query(op.Num)
		return Message{
			Payload: cfg,
			Err:     err,
		}
	default:
		panic(fmt.Sprintf("invalid Operation Type, [op=%v]", op))
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	//sc.configs = make([]Config, 1)
	//sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh, true)

	// Your code here.
	sc.db = NewCfgDB()
	sc.notifyChan = make(map[int]chan Message)
	sc.lastOperations = make(map[int64]Operation)
	sc.lastApplied = 0

	// 开启一个协程来监听applyChan
	go sc.applier()

	return sc

}
