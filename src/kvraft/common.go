package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
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

// =============区分request的类型================//
type OptType int

const (
	GET OptType = iota
	PUT
	APPEND
)

// ==============Cmd=[Get,Put,Append]=================//
type CmdArgs struct {
	Op       OptType //cmd的类型
	Key      string  //cmd的操作key
	Value    string  //cmd的操作可能需要的value
	ClientId int64   //发送cmd的client id
	Seq      int64   //标识cmd的seq
}
type CmdReply struct {
	Status Err    //错误类型
	Value  string //返回值
}

// ==================Message: applier和client间传递的消息类型==============//
type Message struct {
	Payload string
	Err     Err
}
