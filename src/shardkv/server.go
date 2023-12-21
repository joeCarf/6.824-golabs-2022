package shardkv

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	"github.com/sasha-s/go-deadlock"
	"sync/atomic"
	"time"
)

const ExecutionTimeout = 500 * time.Millisecond

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op       OpType //Opt的类型
	Key      string //Opt的key
	Value    string //Opt的val
	ClientId int64  //提交opt的client
	Seq      int64  //该opt的seq
}

//======将opt+seq封装为了一个新结构operation
type Operation Op

type ShardKV struct {
	mu           deadlock.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	sc             *shardctrler.Clerk
	dead           int32
	lastApplied    int                         // 记录了raft中的lastApplied, 防止apply的顺序, 导致日志的回滚
	db             ShardKVdb                   //底层的线程安全的kv数据库
	notifyChan     map[int]chan OperationReply //index->chan的map, 用来通知client协程, applier已经响应了
	lastOperations map[int64]Operation         //clientId->Operation, 为每个client维护一个最后执行的operation的map, 方便去重

	lastConfig    shardctrler.Config //记录当前的上一个config, 主要是为了pull和push阶段使用
	currentConfig shardctrler.Config //当前的config
}

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
func (kv *ShardKV) isDuplicatedCommand(seq int64, clientId int64) (bool, string) {
	// 从map中找到client对应的最新apply的cmd, 如果该kv不存在，则返回false
	lastOperation, ok := kv.lastOperations[clientId]
	if ok {
		return seq == lastOperation.Seq, lastOperation.Value
	} else {
		return false, ""
	}
}
func (kv *ShardKV) isOutDatedCommand(seq int64, clientId int64) bool {
	lastOperation, ok := kv.lastOperations[clientId]
	if ok {
		return lastOperation.Seq > seq
	} else {
		return false
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Command{})
	labgob.Register(OperationArgs{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(ShardsArgs{})
	labgob.Register(ShardsReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.sc = shardctrler.MakeClerk(ctrlers)
	kv.dead = 0
	kv.notifyChan = make(map[int]chan OperationReply)
	kv.db = NewShardKVdb()
	kv.lastOperations = make(map[int64]Operation)
	kv.lastConfig = shardctrler.DefaultConfig()
	kv.currentConfig = shardctrler.DefaultConfig()

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)

	log := false
	if kv.gid == 101 {
		log = true
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh, log)

	//crash时记得恢复快照
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.installSnapshot(snapshot)
	}

	// 开启一个后台协程来监听applyChan
	go kv.applier()

	go kv.tickerConfiguration()
	go kv.tickerPullShard()
	go kv.tickerGarbageCollection()

	DPrintf(DLog, "server-%d-%d start server!", kv.gid, kv.me)
	return kv
}
