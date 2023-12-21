package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrOutofDate   = "ErrOutofDate"
)

type Err string

// Put or Append
//type PutAppendArgs struct {
//	Key   string
//	Value string
//	Op    string // "Put" or "Append"
//	// You'll have to add definitions here.
//	// Field names must start with capital letters,
//	// otherwise RPC will break.
//	ClientId int64
//	Seq      int64
//}
//
//type PutAppendReply struct {
//	Err Err
//}
//
//type GetArgs struct {
//	Key string
//	// You'll have to add definitions here.
//	ClientId int64
//	Seq      int64
//}
//
//type GetReply struct {
//	Err   Err
//	Value string
//}

// =============Operation: 区分request的类型================//
// ==============Operation=[Get,Put,Append]=================//
type OpType int

const (
	GET OpType = iota
	PUT
	APPEND
)

type OperationArgs struct {
	Op       OpType //cmd的类型
	Key      string //cmd的操作key
	Value    string //cmd的操作可能需要的value
	ClientId int64  //发送cmd的client id
	Seq      int64  //标识cmd的seq
}
type OperationReply struct {
	Status Err    //错误类型
	Value  string //返回值
}

//==================Command: 在raft层共识的log类型=======================//
//==================Command=[Operation, Config, Shards]================//
type CmdType int

const (
	OPERATION CmdType = iota
	CONFIG
	//SHARDS 开始想用一个shards统一, 区分ADD和REMOVE两种, 但是二者对应的args不同, 只能作罢
	ADDSHARDS
	REMOVESHARDS
	NOOP
)

type Command struct {
	Cmd     CmdType
	CmdArgs interface{}
}

//==================Shards: 需要向其他raft请求Shards的rpc================//

type ShardsArgs struct {
	ShardIds  []int
	ConfigNum int
}
type ShardsReply struct {
	Shards         map[int]KVdb        //gid->shards的数据
	ConfigNum      int                 //configNum
	Status         Err                 //shards的状态
	LastOperations map[int64]Operation //kv.lastOperations, 防止cmd和shard乱序到达导致的二次提交
}

//// ==================Message: applier和client间传递的消息类型==============//
//type Message struct {
//	Payload string
//	Err     Err
//}
