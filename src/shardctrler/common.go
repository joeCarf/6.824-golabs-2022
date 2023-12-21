package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrOutofDate   = "ErrOutofDate"
)

type Err string

// =============区分request的类型================//
type OptType int

const (
	JOIN OptType = iota
	LEAVE
	MOVE
	QUERY
)

// ==============Cmd=[Get,Put,Append]=================//
type CmdArgs struct {
	Op       OptType          //cmd的类型
	Servers  map[int][]string //for join
	GIDs     []int            //for leave
	Shard    int              //for move
	GID      int              //for move
	Num      int              //for query
	ClientId int64            //发送cmd的client id
	Seq      int64            //标识cmd的seq
}
type CmdReply struct {
	Status Err    //错误类型
	Config Config //返回值
}

// ==================Message: applier和client间传递的消息类型==============//
type Message struct {
	Payload Config
	Err     Err
}

func DefaultConfig() Config {
	return Config{
		Num:    0,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}
}

//const (
//	OK = "OK"
//)
//
//type Err string
//
//type JoinArgs struct {
//	Servers map[int][]string // new GID -> servers mappings
//}
//
//type JoinReply struct {
//	WrongLeader bool
//	Err         Err
//}
//
//type LeaveArgs struct {
//	GIDs []int
//}
//
//type LeaveReply struct {
//	WrongLeader bool
//	Err         Err
//}
//
//type MoveArgs struct {
//	Shard int
//	GID   int
//}
//
//type MoveReply struct {
//	WrongLeader bool
//	Err         Err
//}
//
//type QueryArgs struct {
//	Num int // desired config number
//}
//
//type QueryReply struct {
//	WrongLeader bool
//	Err         Err
//	Config      Config
//}
