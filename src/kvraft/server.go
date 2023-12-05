package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"sync"
	"sync/atomic"
	"time"
)

//const Debug = 0
//
//func DPrintf(format string, a ...interface{}) (n int, err error) {
//	if Debug > 0 {
//		log.Printf(format, a...)
//	}
//	return
//}

const ExecutionTimeout = 2 * time.Second

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Opt      OptType //Opt的类型
	Key      string  //Opt的key
	Value    string  //Opt的val
	ClientId int64   //提交opt的client
	Seq      int64   //该opt的seq
}

// 定义线程安全的kv database
type KVdb struct {
	mu sync.RWMutex      //保证互斥访问的锁
	db map[string]string //用一个map来模拟kv数据库
}

// NewKVdb
//
//	@Description: KVdb的构造函数
//	@return *KVdb
func NewKVdb() KVdb {
	return KVdb{
		mu: sync.RWMutex{},
		db: make(map[string]string),
	}
}

// Get
//
//	@Description: 从map中Get一个值, 如果key不存在, 返回null
//	@receiver k
//	@param key
//	@return string
//	@return Err
func (k *KVdb) Get(key string) (string, Err) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	ret, ok := k.db[key]
	if !ok {
		return "", ErrNoKey
	}
	return ret, OK
}

// Put
//
//	@Description: 替换key的值为val, 如果不存在则创建一个新的
//	@receiver k
//	@param key
//	@param val
func (k *KVdb) Put(key string, val string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.db[key] = val
}

// Append
//
//	@Description: 增加args到key的值上, 如果不存在创建一个新的
//	@receiver k
//	@param key
//	@param val
func (k *KVdb) Append(key string, args string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.db[key] += args
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db         KVdb //底层的线程安全的kv数据库
	notifyChan map[int]chan Message
}

func (kv *KVServer) HandlerCommand(args *CmdArgs, reply *CmdReply) {
	kv.mu.RLock()
	me := kv.me
	kv.mu.RUnlock()
	// 包装一个command, 并调用raft.start来执行这个cmd
	op := Op{
		Opt:      args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	}
	index, _, isLeader := kv.rf.Start(op)
	// 如果不是leader, 返回ErrWrongLeader
	if !isLeader {
		DPrintf(dCommand, "R%d -> C%v refuse for not leader!", me, args.ClientId)
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf(dCommand, "R%d -> C%v leader start a command, op=%v", me, args.ClientId, op)
	// 创建一个index对应的chan用来接受这个index的返回值
	kv.mu.Lock()
	kv.notifyChan[index] = make(chan Message)
	ch := kv.notifyChan[index]
	kv.mu.Unlock()
	// 阻塞等待返回值或者执行时间超时
	select {
	case msg := <-ch:
		reply.Err, reply.Value = msg.Err, msg.Payload
		DPrintf(dGet, "R%d -> C%v finish command success, and reply=%v", me, args.ClientId, reply)
	case <-time.After(ExecutionTimeout):
		reply.Err, reply.Value = ErrTimeout, ""
		DPrintf(dGet, "R%d -> C%v execute command time out!", me, args.ClientId)
	}
	// 不管是正常结束还是超时, 最后要释放掉这个chan
	kv.mu.Lock()
	delete(kv.notifyChan, index)
	kv.mu.Unlock()
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
func (kv *KVServer) applier() {
	kv.mu.RLock()
	me := kv.me
	kv.mu.RUnlock()
	// 只要kvserver未停止就一直监听
	for !kv.killed() {
		// 阻塞的从applyCh读取rf传递过来的消息
		applyMsg := <-kv.applyCh
		op := applyMsg.Command.(Op) // 通过类型断言, 将接口转为Op类型
		index := applyMsg.CommandIndex
		// 将这个已经commit掉的日志应用到数据库里
		msg := kv.applyToDatabase(op)
		// 将返回值通过ch通知对应的client发送端
		kv.mu.Lock()
		ch := kv.notifyChan[index]
		kv.mu.Unlock()
		DPrintf(dApply, "R%d applied cmd to database, reply to client, op=%v", me, op)
		ch <- msg
	}
}

// applyToDatabase
//
//	@Description: 根据op的类型, 将操作执行到状态机里, 也就是在数据库里执行对应的操作
//	@receiver kv
//	@param op
//	@return CommandReply
func (kv *KVServer) applyToDatabase(op Op) Message {
	// 线程安全的db, 不用加锁
	kv.mu.RLock()
	me := kv.me
	kv.mu.RUnlock()
	var msg Message
	switch op.Opt {
	case GET:
		val, err := kv.db.Get(op.Key)
		msg.Err, msg.Payload = err, val
		//DPrintf(dApply, "R%d applied cmd to db, op=%v", me, op)
	case PUT:
		kv.db.Put(op.Key, op.Value)
		//DPrintf(dApply, "R%d applied cmd to db, op=%v", me, op)
	case APPEND:
		kv.db.Append(op.Key, op.Value)
		//DPrintf(dApply, "R%d applied cmd to db, op=%v", me, op)
	default:
		DPrintf(dApply, "R%d apply cmd to db failed, unexpected opt type, op=%v", me, op)
	}
	return msg
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.notifyChan = make(map[int]chan Message)
	kv.db = NewKVdb()

	// 开启一个后台协程来监听applyChan
	go kv.applier()

	return kv
}
