package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"github.com/sasha-s/go-deadlock"
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

const ExecutionTimeout = 500 * time.Millisecond

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

// 定义线程安全的kv database, 就是底层的state machine
// 这里相当于直接定义的内存里的kv, 实际上在生产级别的 KV 服务中，数据不可能全存在内存中，系统往往采用的是 LSM 的架构，例如 RocksDB.
type KVdb struct {
	mu deadlock.RWMutex  //保证互斥访问的锁
	db map[string]string //用一个map来模拟kv数据库
}

// NewKVdb
//
//	@Description: KVdb的构造函数
//	@return *KVdb
func NewKVdb() KVdb {
	return KVdb{
		mu: deadlock.RWMutex{},
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

//======将opt+seq封装为了一个新结构operation
type Operation Op

type KVServer struct {
	mu      deadlock.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied    int                  // 记录了raft中的lastApplied, 防止apply的顺序, 导致日志的回滚
	db             KVdb                 //底层的线程安全的kv数据库
	notifyChan     map[int]chan Message //index->chan的map, 用来通知client协程, applier已经响应了
	lastOperations map[int64]Operation  //clientId->Operation, 为每个client维护一个最后执行的operation的map, 方便去重
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
func (kv *KVServer) isDuplicatedCommand(seq int64, clientId int64) (bool, string) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	// 从map中找到client对应的最新apply的cmd, 如果该kv不存在，则返回false
	lastOperation, ok := kv.lastOperations[clientId]
	if ok {
		return seq == lastOperation.Seq, lastOperation.Value
	} else {
		return false, ""
	}
}
func (kv *KVServer) isOutDatedCommand(seq int64, clientId int64) bool {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	lastOperation, ok := kv.lastOperations[clientId]
	if ok {
		return lastOperation.Seq > seq
	} else {
		return false
	}
}

func (kv *KVServer) HandlerCommand(args *CmdArgs, reply *CmdReply) {
	kv.mu.RLock()
	me := kv.me
	kv.mu.RUnlock()
	// rpc去重, 如果收到重复的写rpc, 直接返回而不对db进行操作
	// NOTE: 必须用ok判断map是否存在, 否则直接调用会默认创建一个新的
	//if args.Op != GET && args.Seq == kv.lastOperations[args.ClientId].Seq {
	if isDuplicated, value := kv.isDuplicatedCommand(args.Seq, args.ClientId); args.Op != GET && isDuplicated {
		//收到了重复的RPC, 直接返回
		reply.Status, reply.Value = OK, value
		DPrintf(dCommand, "R%d -> C%v received duplicate command, return, [args=%v, reply=%v]",
			me, args.ClientId, args, reply)
		return
	}
	// 包装一个command, 并调用raft.start来执行这个cmd
	op := Op{
		Opt:      args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	}
	// NOTE: 日志提交的时候不需要持锁, 因为是交给raft层的Start()实现的, raft层已经保证了互斥, 这里没必要阻塞
	index, _, isLeader := kv.rf.Start(op)
	// 如果不是leader, 返回ErrWrongLeader
	if !isLeader {
		DPrintf(dCommand, "R%d -> C%v refuse for not leader!", me, args.ClientId)
		reply.Status, reply.Value = ErrWrongLeader, ""
		return
	}
	DPrintf(dCommand, "R%d -> C%v leader start a command, op=%v", me, args.ClientId, op)
	// 创建一个index对应的chan用来接受这个index的返回值
	kv.mu.Lock()
	kv.notifyChan[index] = make(chan Message)
	ch := kv.notifyChan[index]
	kv.mu.Unlock()
	// NOTE: 这里必须得做一个超时处理, 防止client协程一直阻塞;
	// 阻塞等待返回值或者执行时间超时
	select {
	case msg := <-ch:
		reply.Status, reply.Value = msg.Err, msg.Payload
		DPrintf(dCommand, "R%d -> C%v finish command success, and reply=%v", me, args.ClientId, reply)
	case <-time.After(ExecutionTimeout):
		reply.Status, reply.Value = ErrTimeout, ""
		DPrintf(dCommand, "R%d -> C%v execute command time out!", me, args.ClientId)
	}
	// 不管是正常结束还是超时, 最后要释放掉这个chan
	//NOTE: 这里可以另起一个协程来释放, 可以增加吞吐量, 没必要阻塞在这里
	go func(index int) {
		kv.mu.Lock()
		delete(kv.notifyChan, index)
		kv.mu.Unlock()
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
func (kv *KVServer) applier() {
	kv.mu.RLock()
	me := kv.me
	kv.mu.RUnlock()
	// 只要kvserver未停止就一直监听
	for !kv.killed() {
		// 阻塞的从applyCh读取rf传递过来的消息
		// NOTE: applier收到的msg有两种: 一种是client的leader主动apply的, 另一种是follower收到leader同步给它的
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid == true {
			op := applyMsg.Command.(Op) // 通过类型断言, 将接口转为Op类型
			index := applyMsg.CommandIndex
			term := applyMsg.CommandTerm
			kv.mu.Lock()
			// NOTE: 要小心日志的rollback, 因为raft去applyCh是不能保证原子性的, chan里的日志可能是乱序的, 所以要有一个lastApplied来保证不会回滚
			// 如果收到的是一个过时的旧日志, 直接抛弃, 避免出现log rollback
			if index <= kv.lastApplied {
				DPrintf(dApply, "R%d received out of date command and drop it. [applyMsg=%v]", me, applyMsg)
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = index
			kv.mu.Unlock()
			// 将这个已经commit掉的日志应用到数据库里
			// NOTE: 在应用状态机之前, 务必检查是否是重复操作, 保持线性一致性, 而且必须在这里检查而不能在handler处检查, 因为follower的状态机不是通过handler而是通过leader的apply同步过来的.
			// NOTE: 只需要去重写请求即可, 读请求重复不会有一致性问题
			var msg Message
			// NOTE: 除了重复, 还必须抛弃过时的command, 防止覆盖状态机引起不一致问题
			if isOutDated := kv.isOutDatedCommand(op.Seq, op.ClientId); isOutDated {
				DPrintf(dApply, "R%d received out of dated command. [op=%v]", me, op)
				msg.Payload, msg.Err = "", ErrOutofDate
				return
			}
			if isDuplicated, value := kv.isDuplicatedCommand(op.Seq, op.ClientId); op.Opt != GET && isDuplicated {
				// 如果是重复的操作, 直接从最新的写操作里面拿到最新的写值即可
				DPrintf(dApply, "R%d received duplicate command. [op=%v]", me, op)
				msg.Payload, msg.Err = value, ""
			} else {
				// 不重复的最新操作, 才需要应用到状态机
				DPrintf(dApply, "R%d applied cmd to database, op=%v", me, op)
				msg = kv.applyToDatabase(op)
				kv.mu.Lock()
				// 只有写操作才需要更新lastOperation, 因为读操作不会引起一致性问题
				if op.Opt != GET {
					kv.lastOperations[op.ClientId] = Operation{
						Opt:      op.Opt,
						Key:      op.Key,
						Value:    op.Value,
						ClientId: op.ClientId,
						Seq:      op.Seq,
					}
				}
				kv.mu.Unlock()
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
					DPrintf(dApply, "R%d send apply reply to client, op=%v", me, op)
				}

			}
			kv.mu.Lock()
			// 判断是否需要快照, 当需要快照, 且本地log超过maxraftstate的时候需要建立快照
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
				//对KVServer中的特有的结构进行快照
				snapshot := kv.makeSnapshot()
				kv.rf.Snapshot(index, snapshot)
				DPrintf(dApply, "R%d over maxraftstate call rf.Snapshot to install snapshot, index=%v", me, index)
			}
			kv.mu.Unlock()
		} else if applyMsg.SnapshotValid == true {
			//follower收到了raft传来的建立快照的消息, 建立对应的快照
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
				kv.installSnapshot(applyMsg.Snapshot)
				DPrintf(dApply, "R%d install snapshot from leader, [msg=%v]", kv.me, applyMsg)
				// 从快照中更新kv后, 不要忘记更新kv.lastApplied
				kv.lastApplied = applyMsg.SnapshotIndex
			}
			kv.mu.Unlock()
		} else {
			DPrintf(dApply, "R%d received unknown message, [msg=%v]", me, applyMsg)
		}
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
	//kv.mu.RLock()
	//me := kv.me
	//kv.mu.RUnlock()
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
		//DPrintf(dApply, "R%d apply cmd to db failed, unexpected opt type, op=%v", me, op)
	}
	return msg
}

//========================快照持久化相关========================//
//
// persistWithSnapshot
//  @Description: 将KVServer的结构序列化为字节流
//  @receiver kv
//  @return []byte
//
func (kv *KVServer) makeSnapshot() []byte {
	// 持久化KVServer中的结构, 包括KVdb和
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.lastApplied) != nil ||
		e.Encode(kv.db.db) != nil ||
		e.Encode(kv.lastOperations) != nil {
		DPrintf(dError, "R%d encode fail", kv.me)
	}
	return w.Bytes()
}

//
// installSnapshot
//  @Description: 安装快照, 就是反序列化字节流
//  @receiver kv
//  @param snapshot
//
func (kv *KVServer) installSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		kv.lastApplied = 0
		kv.db.db = make(map[string]string)
		kv.lastOperations = make(map[int64]Operation)
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var lastApplied int
	var db map[string]string
	var lastOperations map[int64]Operation

	if d.Decode(&lastApplied) != nil ||
		d.Decode(&db) != nil ||
		d.Decode(&lastOperations) != nil {
		DPrintf(dError, "R%d decode fail", kv.me)
		panic("encode fail")
	}
	kv.lastApplied = lastApplied
	kv.db.db = db
	kv.lastOperations = lastOperations
	DPrintf(dSnapshot, "R%d read persist: [lastApplied=%v, db=%v, lastOperations=%v]",
		kv.me, lastOperations, db, lastOperations)
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
	kv.lastOperations = make(map[int64]Operation)

	//crash时记得恢复快照
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.installSnapshot(snapshot)
	}

	// 开启一个后台协程来监听applyChan
	go kv.applier()

	return kv
}
