package raft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       ServerStatus //当前节点的状态
	currentTerm int          //当前任期号
	votedFor    int          //投票给了谁

	heartbeatTimer *time.Timer //心跳包超时计时器
	electionTimer  *time.Timer //超时选举计时器
	// (2B)
	log SafeEntryLog //日志项

	commitIndex int //当前已提交的日志最新索引
	lastApplied int //当前以应用到状态机的日志最新索引

	nextIndex  []int //为每个节点维护的下一个要发送给follower日志条目
	matchIndex []int //记录自身和所有follower日志能匹配到的位置

	applyCond *sync.Cond    //用来通知applyChan来提交的cond
	applyChan chan ApplyMsg //用来提交状态机的applyCh

	//lastIncludeIndex //二者就包含在日志的第0项中
	//lastIncludeTerm
}

//raft中节点的状态
type ServerStatus int

const (
	FOLLOWER ServerStatus = iota
	CANDIDATE
	LEADER
)

//--------------log entry相关的结构--------------//
type Entry struct {
	Term     int         //日志的term号
	Index    int         //日志的索引
	MetaData interface{} //元数据
}

//NOTE: 想了一下, 还是不封装多个锁了, 操作多个锁, 容易导致死锁, 就尽量用一个把
type SafeEntryLog struct {
	Entries []Entry
	//offset  int //logic到physic的偏移
}

//
// lastEntry
//  @Description: 获得log的最后一个Entry
//  @receiver s
//  @return *Entry
//
func (s *SafeEntryLog) lastEntry() *Entry {
	n := len(s.Entries)
	return &s.Entries[n-1]
}

//
// lastIncludeEntry
//  @Description: log的lastIncludeEntry
//  @receiver s
//  @return *Entry
//
func (s *SafeEntryLog) lastIncludeEntry() *Entry {
	return &s.Entries[0]
}

func (s *SafeEntryLog) phy2logIndex(index int) int {
	return index - s.lastIncludeEntry().Index
}

//
// at
//  @Description: 通过实际地址访问log
//  @receiver s
//  @param index
//  @return Entry
//
func (s *SafeEntryLog) at(index int) Entry {
	logIndex := index - s.lastIncludeEntry().Index
	return s.Entries[logIndex]
}

//type SafeEntry struct {
//}

//
// appendNewEntryToLog
//  @Description: 生成一个entry包含这个command, 并插入log里
//  @receiver rf
//  @param commmand 元数据
//  @return int 这个日志的index
//
func (rf *Raft) appendNewEntryToLog(command interface{}) int {
	// 还是这个问题, 锁的粒度还是保持尽可能大, 因为这里进来后, 有可能不再是leader了
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.state != LEADER {
		return -1
	}
	e := Entry{
		Term:     rf.currentTerm,
		Index:    rf.log.lastEntry().Index + 1,
		MetaData: command,
	}
	rf.log.Entries = append(rf.log.Entries, e)
	// 每次更新日志的地方, 不要忘了同步更新nextIndex和matchIndex
	rf.nextIndex[rf.me] = e.Index + 1
	rf.matchIndex[rf.me] = e.Index
	DPrintf(dStart, "T%d: S%d received a new command to replicate, append to log: entry=%v", rf.currentTerm, rf.me, e)
	return e.Index
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	return term, isleader
}

//// return 当前term是否有日志
func (rf *Raft) GetIsLogInCurrentTerm() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.log.lastEntry().Term == rf.currentTerm
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	//rf.mu.RLock()
	//defer rf.mu.RUnlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.log) != nil ||
		e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil {
		DPrintf(dError, "T%d: S%d encode fail", rf.currentTerm, rf.me)
	}
	raftState := w.Bytes()
	rf.persister.SaveRaftState(raftState)
}
func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	//NOTE: 快照的持久化尽量不上锁, 因为释放在拿锁会被别的抢走
	//rf.mu.RLock()
	//defer rf.mu.RUnlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.log) != nil ||
		e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil {
		DPrintf(dError, "T%d: S%d encode fail", rf.currentTerm, rf.me)
	}
	raftState := w.Bytes()
	rf.persister.SaveStateAndSnapshot(raftState, snapshot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log SafeEntryLog
	if d.Decode(&log) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil {
		DPrintf(dError, "T%d: S%d decode fail", rf.currentTerm, rf.me)
		panic("encode fail")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	//copy(rf.log.Entries, log.Entries) //复制切片采用深拷贝
	rf.log = log
	rf.commitIndex = rf.log.lastIncludeEntry().Index
	rf.lastApplied = rf.log.lastIncludeEntry().Index
	DPrintf(dPersist, "T%d: S%d read persist: state=%v,[currentTerm=%v, votedFor=%v, log=%v]",
		rf.currentTerm, rf.me, rf.state, rf.currentTerm, rf.votedFor, rf.log)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果index大于commitIndex, 说明还没commit, 所以不能安装快照
	if index > rf.commitIndex {
		DPrintf(dSnapshot, "T%d: S%d cannot Snapshot() for not committed. [index=%v, commitIndex=%v]",
			rf.currentTerm, rf.me, index, rf.commitIndex)
		return
	}
	// 如果index小于快照点lastIncludeIndex, 说明已经装过了, 这部分不需要装快照
	if index <= rf.log.lastIncludeEntry().Index {
		DPrintf(dSnapshot, "T%d: S%d cannot Snapshot() for has snapshotted. [index=%v, lastIncludeIndex=%v]",
			rf.currentTerm, rf.me, index, rf.log.lastIncludeEntry().Index)
		return
	}
	// 后面需要进行建立快照
	beforeIndex := rf.log.lastIncludeEntry().Index
	logIndex := rf.log.phy2logIndex(index)
	rf.log.Entries = rf.log.Entries[logIndex:]
	DPrintf(dSnapshot, "T%d: S%d create Snapshot. snapshot:[%v->%v], [log=%v], [rf.commitIndex=%v, rf.lastApplied=%v]",
		rf.currentTerm, rf.me, beforeIndex, index, rf.log.Entries, rf.commitIndex, rf.lastApplied)
	//rf.mu.Unlock()
	//// persist之前, 务必要释放锁
	//rf.persistWithSnapshot(snapshot)
	//// NOTE: 这里没有通知applier去提交快照, 放到心跳包里去做了, 也可以对快照另起一个applier以作区分
	//DPrintf(dLog, "T%d: S%d here1!", rf.currentTerm, rf.me)
	//rf.mu.Lock()
	//DPrintf(dLog, "T%d, S%d here2!", rf.currentTerm, rf.me)

	rf.persistWithSnapshot(snapshot)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate的term
	CandidateId  int //candidate的id
	LastLogIndex int //candidate的last log entry index
	LastLogTerm  int //candidate的last log entry term
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //接受者的term, 用来让candidate可以更新自己的状态
	VoteGranted bool //是否投票给自己
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	//reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		DPrintf(dVote, "T%d: S%d -> S%d Vote refused for small term: [candidateTerm=%d, currentTerm=%d]",
			rf.currentTerm, rf.me, args.CandidateId, args.Term, rf.currentTerm)
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	//server rule: 只要遇到更新的term号，自己就要变为FOLLOWER，并且更新自己的Term
	if args.Term > rf.currentTerm {
		DPrintf(dVote, "T%d: S%d <- S%d receive forward term, convert to follower: [leaderTerm=%d > currentTerm=%d]",
			rf.currentTerm, rf.me, args.CandidateId, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	//If votedFor is null or candidateId, grant vote
	// 如果自己已经投过票了, 就退出
	//TODO: 为什么这里如果已经投给自己了也算, 感觉只有前边也可以通过
	if !(rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		DPrintf(dVote, "T%d: S%d -> S%d Has voted in this term: [votedFor=%d]", rf.currentTerm, rf.me, args.CandidateId, rf.votedFor)
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	// RV RPC receiver implementation: candidate's log is at least as up-to-date as receiver's log, grant vote
	// candidate的log至少要和自己的一样up-to-date, 才能投给它
	// 对up-to-date的理解:
	//1. 你的latestTerm比我大
	//2. 你的latestTerm和我一样，但是你的latestIndex 大于等于我的
	if !((args.LastLogTerm > rf.log.lastEntry().Term) ||
		(args.LastLogTerm == rf.log.lastEntry().Term && args.LastLogIndex >= rf.log.lastEntry().Index)) {
		DPrintf(dVote, "T%d: S%d -> S%d Vote refused for not up-to-date: [candidate.LastLogEntry=[%d,%d], lastEntry=%v]",
			rf.currentTerm, rf.me, args.CandidateId, args.LastLogIndex, args.LastLogTerm, rf.log.lastEntry())
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	//投票给他
	DPrintf(dVote, "T%d: S%d -> S%d Vote for candidate", rf.currentTerm, rf.me, args.CandidateId)
	rf.votedFor = args.CandidateId
	rf.electionTimer.Reset(rf.randomElectionTime())
	reply.Term, reply.VoteGranted = rf.currentTerm, true
	return
}

//==================AppendEntries RPC相关====================//
type AppendEntriesArgs struct {
	Term         int     //leader的term
	LeaderId     int     //leader的id
	PrevLogIndex int     //leader需要同步的日志列表的前一条日志index
	PrevLogTerm  int     //leader需要同步的日志列表的前一条日志term
	Entries      []Entry //leader同步的log entries
	LeaderCommit int     //leader's commitIndex
}
type AppendEntriesReply struct {
	Term    int  //接收者的term, 让leader可以更新自己状态
	Success bool //AP操作是否成功

	//用于log fast backup
	XTerm  int //冲突日志项的term
	XIndex int //index of first entry with that term (if any)
	XLen   int //log length
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// AE RPC receiver implementation: reply false if term < currentTerm
	// 如果收到了落后的term, 回复false并让它更新
	if args.Term < rf.currentTerm {
		DPrintf(dAppend, "T%d: S%d <- S%d refused AppendEntries from leader for backward term: [leaderTerm=%d < currentTerm=%d]",
			rf.currentTerm, rf.me, args.LeaderId, args.Term, rf.currentTerm)
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	//server rule: 如果收到了更新的请求, convert to follower
	if args.Term > rf.currentTerm {
		DPrintf(dAppend, "T%d: S%d <- S%d receive forward term, convert to follower: [leaderTerm=%d > currentTerm=%d]",
			rf.currentTerm, rf.me, args.LeaderId, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	// 认可了leader的地位, 就更新选举计时器
	rf.electionTimer.Reset(rf.randomElectionTime())

	// AE RPC receiver implementation: reply false if log does not contain an entry at PrevLogIndex whose term match PrevLogTerm
	// 如果包含的log的entry, index有, 但是term对不上, 说明里面的东西不同, return false
	//log fast backup, 增加了日志快速恢复
	//这里因为加了快照, 必须要判断PrevLogIndex是否大于lastIncludeIndex
	if !(args.PrevLogIndex <= rf.log.lastEntry().Index &&
		(args.PrevLogIndex >= rf.log.lastIncludeEntry().Index && rf.log.at(args.PrevLogIndex).Term == args.PrevLogTerm)) {
		//if args.PrevLogIndex > rf.log.lastEntry().Index {
		//	DPrintf(dAppend, "T%d: S%d <- S%d refused AppendEntries from leader for mismatch PrevLogEntry: args[%d,%d] is not exist",
		//		rf.currentTerm, rf.me, args.LeaderId, args.PrevLogTerm, args.PrevLogIndex)
		//	// 如果follower的log太短, 就没有PrevLogEntry这一项, 通过XLen来处理
		//	reply.XTerm, reply.XIndex, reply.XLen = -1, -1, rf.log.lastEntry().Index+1
		//} else {
		//	DPrintf(dAppend, "T%d: S%d <- S%d refused AppendEntries from leader for mismatch PrevLogEntry: args[%d,%d] != entry[%d,%d]",
		//		rf.currentTerm, rf.me, args.LeaderId, args.PrevLogTerm, args.PrevLogIndex, rf.log.Entries[args.PrevLogIndex].Term, rf.log.Entries[args.PrevLogIndex].Index)
		//	// 如果是因为follower和leader的PrevLogEntry不匹配Term, 找到conflict位置的term的第一个出现的位置
		//	conflictTerm := rf.log.Entries[args.PrevLogIndex].Term
		//	conflictIndex := args.PrevLogIndex
		//	for i := conflictIndex; i >= 0; i-- {
		//		if rf.log.Entries[i].Term != conflictTerm {
		//			break
		//		}
		//		conflictIndex = i
		//	}
		//	reply.XTerm, reply.XIndex, reply.XLen = conflictTerm, conflictIndex, rf.log.lastEntry().Index+1
		//}
		//log fast backup
		reply.XLen = rf.log.lastEntry().Index + 1
		//如果存在PreLogIndex这一项，XTerm等于这一项的term
		if args.PrevLogIndex >= rf.log.lastIncludeEntry().Index && args.PrevLogIndex < rf.log.lastEntry().Index+1 {
			reply.XTerm = rf.log.at(args.PrevLogIndex).Term
			for i := args.PrevLogIndex; i >= rf.log.lastIncludeEntry().Index; i-- {
				if rf.log.at(i).Term == reply.XTerm {
					reply.XIndex = i
				} else {
					break
				}
			}
		}
		return
	}
	// AE RPC receiver implementation: If an existing entry conflict with a new one(same index but different term), delete the existing entry and all that follow it
	// 如果现有的entry和leader的冲突, 遵循leader的
	// 就是从PrevLogIndex的下一个开始找, 找到第一个conflict的, 后面的可以全删掉, 然后用leader的新日志换掉你的
	// NOTE: 不理解为什么非要这找一下conflict, 我直接用leader的entries把我的后面的全换掉不可以吗? 回答: 不可以，必须经过检查, 因为可能会有乱序和重复的RPC, 不能不经过检查直接覆盖
	// 比如会出现[1，2]，[1,2,3], 但是后者先到了, 导致log先变成了[1,2,3], 此时[1,2]的RPC到了, 但是不能扔掉此时的3;
	// NOTE: 之前先遍历rf.log的思路是错误的, 会导致[1,2]的RPC把[1,2,3]的RPC替换掉, 先遍历entries可以解决这个问题
	conflict := -1
	for i := range args.Entries {
		//	// 在log中找到第一个conflict的位置, 注意需要rf.log不越界
		if args.PrevLogIndex+1+i >= rf.log.lastEntry().Index+1 ||
			rf.log.at(args.PrevLogIndex+1+i).Term != args.Entries[i].Term {
			conflict = i
			break
		}
	}
	if conflict != -1 {
		// 删掉conflict后面的日志, 并用entries的[conflict,)进行替换
		rf.log.Entries = append(rf.log.Entries[:rf.log.phy2logIndex(args.PrevLogIndex+1+conflict)], args.Entries[conflict:]...)
	}
	//for i := args.PrevLogIndex + 1; i < len(rf.log.Entries); i++ {
	//	// 在log中找到第一个conflict的位置, 注意需要特判一下args.entries不越界
	//	if i-(args.PrevLogIndex+1) >= len(args.Entries) || rf.log.Entries[i].Term != args.Entries[i-(args.PrevLogIndex+1)].Term {
	//		conflict = i
	//		break
	//	}
	//}
	//// 特判一下, 如果PrevLogEntry就是log的lastEntry, 那就得整个复制
	//if args.PrevLogIndex == rf.log.lastEntry().Index {
	//	conflict = rf.log.lastEntry().Index + 1
	//}
	//if conflict != -1 {
	//	// 删掉conflict后面的日志, 并用entries的[conflict,)进行替换
	//	rf.log.Entries = append(rf.log.Entries[:conflict], args.Entries[conflict-(args.PrevLogIndex+1):]...)
	//}
	// AE RPC receiver implementation: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if rf.commitIndex == rf.log.lastEntry().Index {
			DPrintf(dCommit, "T%d: S%d leader commit more than me, but have no entries:[%d->%d]",
				rf.currentTerm, rf.me, rf.commitIndex+1, args.LeaderCommit)
		} else {
			DPrintf(dCommit, "T%d: S%d commit the entries as follower: Entries[%d->%d]",
				rf.currentTerm, rf.me, rf.commitIndex+1, Min(args.LeaderCommit, rf.log.lastEntry().Index))
		}
		rf.commitIndex = Min(args.LeaderCommit, rf.log.lastEntry().Index)
		rf.applyCond.Broadcast()
	}
	// 如果都ok了, 说明append完成, 返回true
	DPrintf(dAppend, "T%d: S%d <- S%d receive AppendEntries and reply true, entries=%v, rf.log=%v.",
		rf.currentTerm, rf.me, args.LeaderId, args.Entries, rf.log.Entries)
	reply.Term, reply.Success = rf.currentTerm, true
	return
}

//===============InstallSnapshot RPC 相关=================//
//InstallSnapshotArgs RPC args structure
type InstallSnapshotArgs struct {
	Term             int    //leader的任期
	LeaderId         int    //leader的Id
	LastIncludeIndex int    //快照最后一个apply的日志index
	LastIncludeTerm  int    //快照最后一个apply的日志term
	Data             []byte //快照的字节流
	//不需要实现offset和done
}

//InstallSnapshotArgs RPC reply structure
type InstallSnapshotReply struct {
	Term int //follower的任期
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// IS RPC receiver implementation: reply false if term < currentTerm
	// 如果收到了落后的term, 回复false并让它更新
	if args.Term < rf.currentTerm {
		DPrintf(dSnapshot, "T%d: S%d <- S%d refused InstallSnapshot from leader for backward term: [leaderTerm=%d < currentTerm=%d]",
			rf.currentTerm, rf.me, args.LeaderId, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		return
	}
	//server rule: 如果收到了更新的请求, convert to follower
	if args.Term > rf.currentTerm {
		DPrintf(dSnapshot, "T%d: S%d <- S%d receive forward term, convert to follower: [leaderTerm=%d > currentTerm=%d]",
			rf.currentTerm, rf.me, args.LeaderId, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		// 这里虽然更新了状态, 但是可以放到后面和snap一起持久化
	}
	// 认可了leader的地位, 就更新选举计时器
	rf.electionTimer.Reset(rf.randomElectionTime())

	//发过来的快照太旧了，已经commit过了, 拒绝掉
	if args.LastIncludeIndex <= rf.log.lastIncludeEntry().Index {
		DPrintf(dSnapshot, "T%d: S%d <- S%d refused InstallSnapshot from leader for old snapshot: [args.LastIncludeIndex=%d <= rf.LastIncludeIndex=%d]",
			rf.currentTerm, rf.me, args.LeaderId, args.LastIncludeIndex, rf.log.lastIncludeEntry().Index)
		return
	}

	//根据快照，把rf.log中的[lastIncludeIndex,args.LastIncludeIndex]提交掉，然后删除
	index := args.LastIncludeIndex
	logIndex := rf.log.phy2logIndex(index)
	// NOTE: 不能像snapshot中rf.log.Entries = rf.log.Entries[phyIndex:]更新log, 因为leader同步的快照, 可能比本地的日志还要长, 也可能你根本没有这一项
	restLog := []Entry{{Index: args.LastIncludeIndex, Term: args.LastIncludeTerm}}
	for i := logIndex + 1; i < len(rf.log.Entries); i++ {
		restLog = append(restLog, rf.log.Entries[i])
	}
	rf.log.Entries = restLog

	// 根据快照更新一些其他的状态量
	// NOTE: 这里必须用max, 防止收到过时的rpc导致的rollback
	rf.commitIndex = Max(index, rf.commitIndex)
	rf.lastApplied = Max(index, rf.lastApplied)
	DPrintf(dSnapshot, "T%d: S%d <- S%d installed the snapshot from leader, [commitIndex=%d, lastApplied=%d, rf.log=%v]",
		rf.currentTerm, rf.me, args.LeaderId, rf.commitIndex, rf.lastApplied, rf.log.Entries)
	// 持久化快照
	rf.persistWithSnapshot(args.Data)
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.log.lastIncludeEntry().Term,
		SnapshotIndex: rf.log.lastIncludeEntry().Index,
	}
	// applyCh之前保证不持锁
	rf.mu.Unlock()

	// apply 快照
	//将新的快照apply掉
	rf.applyChan <- msg

	rf.mu.Lock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//====================处理函数===============================//
//
// leaderElection
//  @Description: 发起选举, 调用RequestVote RPC 请求选票
//  @receiver rf
//
func (rf *Raft) leaderElection() {
	//转candidate需要遵守的规则: Increment currentTerm, Vote for myself, Reset election timers
	rf.mu.Lock()
	DPrintf(dTicker, "T%d: S%d ElectionTimer timeout, leader election started", rf.currentTerm, rf.me)
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.electionTimer.Reset(rf.randomElectionTime())
	rf.mu.Unlock()
	// Send RequestVote RPC to all other servers
	getVotes := 1                       //得到的选票数, 默认自己有一张选票
	finishVotes := 1                    //已经完成投票的节点数
	var leMutex sync.Mutex              //用来互斥访问getVotes
	var condRV = sync.NewCond(&leMutex) //条件变量来检查
	//NOTE: 不可以用waitGroup让主线程等待所有rpc请求都完成, 因为可能出现部分节点crash, 只要有半数就行了
	var finWg sync.WaitGroup
	for peer := range rf.peers {
		if peer == rf.me {
			continue //如果是自己直接跳过
		}
		//异步发送请求
		finWg.Add(1)
		DPrintf(dVote, "T%d: S%d -> S%d send request for votes.", rf.currentTerm, rf.me, peer)
		go func(peer int) {
			defer finWg.Done()
			rf.mu.RLock()
			if rf.state != CANDIDATE {
				rf.mu.RUnlock()
				return
			}
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.log.lastEntry().Index,
				LastLogTerm:  rf.log.lastEntry().Term,
			}
			reply := RequestVoteReply{}
			// 发送RPC之前一定要释放锁, 这种长时间的操作持锁很容易死锁
			rf.mu.RUnlock()
			ok := rf.sendRequestVote(peer, &args, &reply)
			//如果RPC发送失败了, 直接返回了
			if !ok {
				//DPrintf(dVote, "T%d: S%d received no reply from S%d", rf.currentTerm, rf.me, peer)
			}
			//NOTE: 加了一个过期rpc的处理, 比如发现args.term已经小于currentTerm了, 说明这是个过期的, 直接忽略就好, 因为RPC处理过程肯定能长, 因为节点是会crash的
			rf.mu.RLock()
			//TODO: 这里不应该是检查state!=candidate, 应该是==follower, 因为哪怕是变成了leader, 也得接受rpc结果的检查；
			if args.Term < rf.currentTerm || rf.state == FOLLOWER {
				DPrintf(dVote, "T%d: S%d <- S%d received expired rpc reply. [args.Term=%d, currentTerm=%d]", rf.currentTerm, rf.me, peer, args.Term, rf.currentTerm)
				rf.mu.RUnlock()
				return
			}
			rf.mu.RUnlock()
			leMutex.Lock()
			if reply.VoteGranted == true {
				getVotes++ // 如果投票给你, 得票数++
			} else {
				rf.mu.Lock()
				// 如果不投给你, 需要检查是否需要更新状态, 需要转为follower
				if reply.Term > rf.currentTerm {
					DPrintf(dVote, "T%d: S%d <- S%d received newer term %d from servers", rf.currentTerm, rf.me, peer, reply.Term)
					rf.state = FOLLOWER
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					//Student Guide里面说: 只有三个时机需要重置选举时间超时
					//rf.electionTimer.Reset(rf.randomElectionTime())
					rf.persist()
				}
				rf.mu.Unlock()
			}
			finishVotes++
			condRV.Broadcast()
			leMutex.Unlock()
		}(peer)
	}
	leMutex.Lock()
	for getVotes <= len(rf.peers)/2 && finishVotes != len(rf.peers) {
		condRV.Wait()
	}
	var retVotes = getVotes
	leMutex.Unlock()
	// 首先需要保证自己还是candicate, 因为在选举过程中可能会更新状态
	rf.mu.Lock()
	if rf.state == CANDIDATE {
		//如果受到的选票过半, 说明可以转为leader
		if retVotes > len(rf.peers)/2 {
			DPrintf(dVote, "T%d: S%d received majority votes, convert to leader", rf.currentTerm, rf.me)
			rf.state = LEADER
			//NOTE: 这里不可以重置选票, 成为leader之后没有投票权
			//rf.votedFor = -1
			// 重置leader特有的matchIndex和nextIndex
			for peer := range rf.peers {
				rf.nextIndex[peer] = rf.log.lastEntry().Index + 1
				rf.matchIndex[peer] = 0
			}
			rf.matchIndex[rf.me] = rf.log.lastEntry().Index
			//发心跳包通知其他节点自己Leader的身份
			rf.heartbeatTimer.Reset(0 * time.Second)
		}
		//就算失败, 也不需要额外做什么, 因为一个任期内只能选举一次, 失败了就等下一次
	}
	//rf.persist()
	rf.mu.Unlock()
	finWg.Wait()
}

//
// broadcastHeartbeat
//  @Description: 广播心跳
//  @receiver rf
//
func (rf *Raft) broadcastHeartbeat() {
	rf.mu.RLock()
	DPrintf(dTicker, "T%d: S%d HearBeatTimer timeout, broad heart beat started", rf.currentTerm, rf.me)
	rf.mu.RUnlock()
	// NOTE: 这里是没必要必须等待所有rpc都返回, 再继续, 因为后续的逻辑里不需要用到返回结果, 阻塞在这里是无意义的
	//var bhbWg sync.WaitGroup
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			rf.mu.RLock()
			nextIndex := rf.nextIndex[peer]
			lastIncludeIndex := rf.log.lastIncludeEntry().Index
			rf.mu.RUnlock()
			// 区分发送snapshot还是log entry, 就看log中还有没有这一项
			if nextIndex <= lastIncludeIndex {
				//nextIndex这一项已经不在日志中了，需要发送快照
				rf.broadcastSnapshot(peer)
			} else {
				//需要发送的是AppendEntries,区分下是否是心跳包即可
				rf.broadcastEntries(peer)
			}
		}(peer)
	}
	// 等待RPC都结束了再退出, 因为有可能会转变状态
	//bhbWg.Wait()
	//if there exist an N such that N > commitIndex, a majority of matchIndex[i]>=N, and log[N].term = currentTerm, set commitIndex = N
	//本质上就是找, 满足半数match成功的最大的index
	//实现上, 可以将matchIndex sort一下, 排序后第len(peers)/2就是第一个majority的位置, 从这里向前遍历到0, 找到第一个符合条件的index
	rf.mu.Lock()
	if rf.state == LEADER {
		matchindex := make([]int, len(rf.matchIndex))
		copy(matchindex, rf.matchIndex)
		sort.Ints(matchindex)
		majorityIdx := len(matchindex) / 2
		for i := majorityIdx; i >= 0 && matchindex[i] > rf.commitIndex; i-- {
			// 这里的term判断, 保证了leader只能commit自己term内的日志
			if rf.log.at(matchindex[i]).Term == rf.currentTerm {
				DPrintf(dCommit, "T%d: S%d commit the entries as leader: Entries[%d->%d]", rf.currentTerm, rf.me, rf.commitIndex+1, matchindex[i])
				rf.commitIndex = matchindex[i]
				rf.applyCond.Broadcast()
				break
			}
		}
	}
	rf.persist()
	rf.mu.Unlock()
}

//
// broadcastEntries
//  @Description: 广播log entries
//  @receiver rf
//  @param peer
//
func (rf *Raft) broadcastEntries(peer int) {
	rf.mu.RLock()
	// FIXME: 不懂为什么, 后面preLogEntry访问at的时候会出现nextIndex超出lastEntry的问题
	//if rf.state != LEADER || rf.nextIndex[peer] <= rf.log.lastIncludeEntry().Index || rf.nextIndex[peer] > rf.log.lastEntry().Index {
	if rf.state != LEADER || rf.nextIndex[peer] <= rf.log.lastIncludeEntry().Index {
		rf.mu.RUnlock()
		return
	}
	term := rf.currentTerm
	leaderid := rf.me
	preLogEntry := rf.log.at(rf.nextIndex[peer] - 1) //根据nextIndex找到要发送的下一条的前一个
	var entries []Entry
	leadercommit := rf.commitIndex
	//leader rules: if last log index >= nextIndex, send AE RPC with log entries starting at nextIndex
	if rf.log.lastEntry().Index >= rf.nextIndex[peer] {
		//entries = rf.log.Entries[rf.nextIndex[peer]:]		//NOTE:不能直接赋值, 因为go中切片的赋值是直接共享底层数据的数据, 必须用copy
		entries = make([]Entry, len(rf.log.Entries)-rf.log.phy2logIndex(rf.nextIndex[peer]))
		copy(entries, rf.log.Entries[rf.log.phy2logIndex(rf.nextIndex[peer]):])
	}
	rf.mu.RUnlock()
	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     leaderid,
		PrevLogIndex: preLogEntry.Index,
		PrevLogTerm:  preLogEntry.Term,
		Entries:      entries,
		LeaderCommit: leadercommit,
	}
	reply := AppendEntriesReply{}
	DPrintf(dAppend, "T%d: S%d -> S%d send AppendEntries to replicate logs: Entries=%v", term, leaderid, peer, entries)
	ok := rf.sendAppendEntries(peer, &args, &reply)
	if !ok {
		//DPrintf(dAppend, "T%d: S%d received no reply from S%d", rf.currentTerm, rf.me, peer)
		return
	}
	//NOTE: 加了一个过期rpc的处理, 比如发现args.term已经小于currentTerm了, 说明这是个过期的, 直接忽略就好, 因为RPC处理过程肯定能长, 因为节点是会crash的
	// 除了过期rpc, 如果节点状态发生了变化, 比如此时的状态变了, 或者已经安装了快照, 忽略此rpc
	rf.mu.RLock()
	if args.Term < rf.currentTerm || rf.state != LEADER || rf.nextIndex[peer] <= rf.log.lastIncludeEntry().Index {
		DPrintf(dAppend, "T%d: S%d <- S%d received expired rpc reply. [args.Term=%d, currentTerm=%d]", rf.currentTerm, rf.me, peer, args.Term, rf.currentTerm)
		rf.mu.RUnlock()
		return
	}
	rf.mu.RUnlock()
	//如果失败了, 说明对方的term更大, 需要更新状态为follower
	rf.mu.Lock()
	if reply.Success == true {
		//if success, update nextIndex and matchIndex, 如果成功就更新next和match
		//NOTE: 这里不能用rf.log.lastEntry().Index来更新nextIndex和matchIndex, 因为可能存在RPC乱序重复的问题, 要这样更新
		rf.nextIndex[peer] = preLogEntry.Index + len(entries) + 1
		rf.matchIndex[peer] = preLogEntry.Index + len(entries)
		DPrintf(dAppend, "T%d: S%d <- S%d received append true from peer, matchIndex[%v]=%v", rf.currentTerm, rf.me, peer, peer, rf.matchIndex[peer])
	} else {
		// if failed
		if reply.Term > rf.currentTerm {
			// 如果是收到了更新的term, convert to follower
			DPrintf(dAppend, "T%d: S%d <- S%d received newer term %d from servers, convert to follower", rf.currentTerm, rf.me, peer, reply.Term)
			rf.state = FOLLOWER
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			//rf.electionTimer.Reset(rf.randomElectionTime())
		} else {
			// 剩下就是因为日志不匹配而失败
			// if failed because of log inconsistency, decrement nextIndex and retry
			//DPrintf(dAppend, "T%d: S%d <- S%d receive refuse for log inconsistency, decrease nextIndex:[%v->%v]", rf.currentTerm, rf.me, peer, rf.nextIndex[peer], rf.nextIndex[peer]-1)
			//rf.nextIndex[peer] = args.PrevLogIndex
			//实现日志的快速恢复：log fast backup
			DPrintf(dAppend, "T%d: S%d <- S%d receive refuse for log inconsistency, [XTerm=%d, XIndex=%d, Xlen=%d, reply.Term=%v]", rf.currentTerm, rf.me, peer, reply.XTerm, reply.XIndex, reply.XLen, reply.Term)
			rf.nextIndex[peer] = args.PrevLogIndex
			// case3: follower's log is too short: nextIndex=Xlen, follower的log过短, 对应的就是preLogIndex的字段为空
			if rf.nextIndex[peer] >= reply.XLen {
				rf.nextIndex[peer] = reply.XLen
			} else {
				// leader去找自己有没有follower conflict的日志的term
				conflictIndex := rf.nextIndex[peer]
				conflictTerm := reply.XTerm
				for i := rf.nextIndex[peer] - 1; i >= reply.XIndex; i-- {
					//安装快照后, 必须防止越界访问
					if i-rf.log.lastIncludeEntry().Index >= 0 && rf.log.at(i).Term == conflictTerm {
						// case2: leader has XTerm: nextIndex = leader's last entry for XTerm
						break
					}
					// case1: leader does not has XTerm: nextIndex = XIndex
					conflictIndex = i
				}
				rf.nextIndex[peer] = conflictIndex
			}
			DPrintf(dAppend, "T%d: S%d receive refuse for unmatch index, decrease nextIndex:[%v->%v]", rf.currentTerm, rf.me, args.PrevLogIndex+1, rf.nextIndex[peer])
			// 这里只需要修改nextIndex就行, 下次heartbeat的时候自然会重发, 不需要用一个for循环来确保rpc成功.
		}
	}
	rf.persist()
	rf.mu.Unlock()
}

//
// broadcastSnapshot
//  @Description: 广播snapshot
//  @receiver rf
//  @param peer
//
func (rf *Raft) broadcastSnapshot(peer int) {
	rf.mu.RLock()
	if rf.state != LEADER || rf.nextIndex[peer] > rf.log.lastIncludeEntry().Index {
		rf.mu.RUnlock()
		return
	}
	term := rf.currentTerm
	leaderid := rf.me
	lastIncludeIndex := rf.log.lastIncludeEntry().Index
	lastIncludeTerm := rf.log.lastIncludeEntry().Term
	data := rf.persister.ReadSnapshot()
	rf.mu.RUnlock()
	//封装InstallSnapshot RPC的args和reply
	args := InstallSnapshotArgs{
		Term:             term,
		LeaderId:         leaderid,
		LastIncludeIndex: lastIncludeIndex,
		LastIncludeTerm:  lastIncludeTerm,
		Data:             data,
	}
	reply := InstallSnapshotReply{}
	DPrintf(dSnapshot, "T%d: S%d -> S%d send InstallSnapshot to install the snapshot. Snapshot={%d,%d,%d,%d}",
		term, rf.me, leaderid, args.Term, args.LeaderId, args.LastIncludeIndex, args.LastIncludeTerm)
	ok := rf.sendInstallSnapshot(peer, &args, &reply)
	if !ok {
		//DPrintf(dAppend, "T%d: S%d received no reply from S%d", rf.currentTerm, rf.me, peer)
		return
	}
	//NOTE: 加了一个过期rpc的处理, 比如发现args.term已经小于currentTerm了, 说明这是个过期的, 直接忽略就好, 因为RPC处理过程肯定能长, 因为节点是会crash的
	rf.mu.RLock()
	if args.Term < rf.currentTerm || rf.state != LEADER || rf.nextIndex[peer] > rf.log.lastIncludeEntry().Index {
		DPrintf(dSnapshot, "T%d: S%d <- S%d received expired rpc reply. [args.Term=%d, currentTerm=%d]",
			rf.currentTerm, rf.me, peer, args.Term, rf.currentTerm)
		rf.mu.RUnlock()
		return
	}
	rf.mu.RUnlock()
	//如果失败了, 说明对方的term更大, 需要更新状态为follower
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		// 如果是收到了更新的term, convert to follower
		DPrintf(dSnapshot, "T%d: S%d <- S%d received newer term %d from servers, convert to follower",
			rf.currentTerm, rf.me, peer, reply.Term)
		rf.state = FOLLOWER
		rf.currentTerm = reply.Term
		rf.votedFor = -1
	} else {
		//否则默认快照的安全都是成功的, 更新commitIndex和matchIndx
		DPrintf(dSnapshot, "T%d: S%d <- S%d received install true from peer, matchIndex[%d]=%v]",
			rf.currentTerm, rf.me, peer, peer, args.LastIncludeIndex)
		rf.nextIndex[peer] = args.LastIncludeIndex + 1
		rf.matchIndex[peer] = args.LastIncludeIndex
	}
	rf.persist()
	rf.mu.Unlock()
}

//====================重要的后台函数==========================//
//
// ticker
//  @Description: 后台进程, 监视raft节点的计时器, 触发对应的处理函数
//  @receiver rf
//
func (rf *Raft) ticker() {
	//需要保证退出机制, 就是rf节点退出的时候
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			//选举计时器超时, follower的规则: 选举计时器超时, 并且要没有收到其他的选票, 且自己没有投过票
			//总结一下就是能选举的条件是: 不是leader, 且在这个term中没投过票
			rf.mu.RLock()
			canElect := rf.state != LEADER
			rf.mu.RUnlock()
			if canElect {
				//NOTE: 这里需要用另一个线程去执行, 因为如果这个过程耗时较长(比如rpc连不到,要等rpc返回值), 会导致ticker阻塞;
				go rf.leaderElection()
			}
			rf.mu.Lock()
			rf.electionTimer.Reset(rf.randomElectionTime()) //只要触发了, 就自动重置一次
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			// 发送心跳包计时器超时, 需要发送心跳包, 同样只有leader可以发心跳包
			rf.mu.RLock()
			canBroadHB := rf.state == LEADER
			rf.mu.RUnlock()
			if canBroadHB {
				go rf.broadcastHeartbeat()
			}
			rf.mu.Lock()
			rf.heartbeatTimer.Reset(rf.stableHeartbeatTime())
			rf.mu.Unlock()
		}
	}
}

//
// applier
//  @Description: 后台进程, 用来监视raft的commitIndex和lastApplied是否重合, 负责提交commited的log到state machine
//  @receiver rf
//
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		//all servers rule: if commitIndex > lastApplied, increment lastApplied, apply log[lastApplied] to state machine.
		//检查是否需要apply
		for !(rf.commitIndex > rf.lastApplied) {
			rf.applyCond.Wait()
		}
		// 被唤醒来apply log[lastApplied] to state machine
		commitindex := rf.commitIndex
		lastapplied := rf.lastApplied
		entries := make([]Entry, rf.commitIndex-rf.lastApplied)
		copy(entries, rf.log.Entries[rf.log.phy2logIndex(rf.lastApplied+1):rf.log.phy2logIndex(rf.commitIndex+1)])
		rf.mu.Unlock()
		// apply是一个写chan的过程, 写chan可能阻塞, 所以这个过程中不要持锁
		for _, e := range entries {
			rf.applyChan <- ApplyMsg{
				CommandValid: true,
				Command:      e.MetaData,
				CommandIndex: e.Index,
				CommandTerm:  e.Term,
			}
		}
		rf.mu.Lock()
		// 注意这里必须记录的commitindex和lastapplied, 因为applych的过程不是阻塞的, 这个过程中rf.commitIndex可能会发生变化
		DPrintf(dApply, "T%d: S%d applied the log: [%d -> %d]", rf.currentTerm, rf.me, lastapplied+1, commitindex)
		//这里必须用commitIndex而不能用rf.commitIndex，因为有可能在push ch的过程中，更新了rf.commitIndex
		//TODO:解决apply entry和snapshot冲突，防止回滚，lastApplied = max(lastApplied, commitIndex)
		rf.lastApplied = Max(rf.lastApplied, commitindex)
		rf.mu.Unlock()
	}
}

//============================自定义功能函数================================//
//
// RandomElectionTime
//  @Description: 生成选举超时计时器的随机数
//  @receiver rf
//  @return time.Duration
//
func (rf *Raft) randomElectionTime() time.Duration {
	ms := 150 + (rand.Int63() % 150)
	return time.Duration(ms) * time.Millisecond
}

//
// StableHeartbeatTime
//  @Description: 生成固定的心跳包间隔
//  @receiver rf
//  @return time.Duration
//
func (rf *Raft) stableHeartbeatTime() time.Duration {
	ms := 50
	return time.Duration(ms) * time.Millisecond
}

//-------------求最大最小值函数--------------------
const (
	MININT64 = -922337203685477580
	MAXINT64 = 9223372036854775807
)

func Max(nums ...int) int {
	var maxNum int = MININT64
	for _, num := range nums {
		if num > maxNum {
			maxNum = num
		}
	}
	return maxNum
}

func Min(nums ...int) int {
	var minNum int = MAXINT64
	for _, num := range nums {
		if num < minNum {
			minNum = num
		}
	}
	return minNum
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.RLock()
	//DPrintf(dStart, "T%d: S%d received a new command to replicate, start here", rf.currentTerm, rf.me)
	isLeader = rf.state == LEADER
	term = rf.currentTerm
	rf.mu.RUnlock()
	if isLeader == false {
		return index, term, isLeader
	}
	index = rf.appendNewEntryToLog(command)
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg, log bool) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	//if log == false {
	//	// 禁用日志
	//	DebugVerbosity = 0
	//} else {
	//	DebugVerbosity = 1
	//}
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartbeatTimer = time.NewTimer(rf.stableHeartbeatTime()) //只有当了leader才能初始化这个心跳计时器
	rf.electionTimer = time.NewTimer(rf.randomElectionTime())
	// (2B)
	rf.log = SafeEntryLog{
		Entries: []Entry{{Index: 0, Term: 0}}, //要求日志index从1开始, 所以这里插入一个空的0日志;
	}
	rf.commitIndex = 0                        //state: initialized to 0, increases monotonically
	rf.lastApplied = 0                        //state: initialized to 0, increases monotonically
	rf.nextIndex = make([]int, len(rf.peers)) //state: initialized to leader last log index + 1
	for peer := range rf.nextIndex {
		rf.nextIndex[peer] = rf.log.lastEntry().Index + 1
	}
	rf.matchIndex = make([]int, len(rf.peers)) //state: initialized to 0, increases monotonically
	for peer := range rf.matchIndex {
		rf.matchIndex[peer] = 0
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.applyChan = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//开启后台监视进程
	go rf.ticker()
	go rf.applier()

	//DPrintf(dLog, "R%d start raft!", rf.me)

	return rf
}
