package raft

import (
	"6.824/labrpc"
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
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

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
	if !(args.PrevLogIndex <= rf.log.lastEntry().Index && rf.log.Entries[args.PrevLogIndex].Term == args.PrevLogTerm) {
		if args.PrevLogIndex > rf.log.lastEntry().Index {
			DPrintf(dAppend, "T%d: S%d <- S%d refused AppendEntries from leader for mismatch PrevLogEntry: args[%d,%d] is not exist",
				rf.currentTerm, rf.me, args.LeaderId, args.PrevLogTerm, args.PrevLogIndex)
		} else {
			DPrintf(dAppend, "T%d: S%d <- S%d refused AppendEntries from leader for mismatch PrevLogEntry: args[%d,%d] != entry[%d,%d]",
				rf.currentTerm, rf.me, args.LeaderId, args.PrevLogTerm, args.PrevLogIndex, rf.log.Entries[args.PrevLogIndex].Term, rf.log.Entries[args.PrevLogIndex].Index)
		}
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	// AE RPC receiver implementation: If an existing entry conflict with a new one(same index but different term), delete the existing entry and all that follow it
	// 如果现有的entry和leader的冲突, 遵循leader的
	// 就是从PrevLogIndex的下一个开始找, 找到第一个conflict的, 后面的可以全删掉, 然后用leader的新日志换掉你的
	// TODO: 不理解为什么非要这找一下conflict, 我直接用leader的entries把我的后面的全换掉不可以吗?
	conflict := -1
	for i := args.PrevLogIndex + 1; i < len(rf.log.Entries); i++ {
		// 在log中找到第一个conflict的位置, 注意需要特判一下args.entries不越界
		if i-(args.PrevLogIndex+1) >= len(args.Entries) || rf.log.Entries[i].Term != args.Entries[i-(args.PrevLogIndex+1)].Term {
			conflict = i
			break
		}
	}
	// 特判一下, 如果PrevLogEntry就是log的lastEntry, 那就得整个复制
	if args.PrevLogIndex == rf.log.lastEntry().Index {
		conflict = rf.log.lastEntry().Index + 1
	}
	if conflict != -1 {
		// 删掉conflict后面的日志, 并用entries的[conflict,)进行替换
		rf.log.Entries = append(rf.log.Entries[:conflict], args.Entries[conflict-(args.PrevLogIndex+1):]...)
	}
	// AE RPC receiver implementation: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if rf.commitIndex == rf.log.lastEntry().Index {
			DPrintf(dCommit, "T%d: S%d leader commit more than me, but have no entries:[%d->%d]", rf.currentTerm, rf.me, rf.commitIndex+1, args.LeaderCommit)
		} else {
			DPrintf(dCommit, "T%d: S%d commit the entries as follower: Entries[%d->%d]", rf.currentTerm, rf.me, rf.commitIndex+1, Min(args.LeaderCommit, rf.log.lastEntry().Index))
		}
		rf.commitIndex = Min(args.LeaderCommit, rf.log.lastEntry().Index)
		rf.applyCond.Broadcast()
	}
	// 如果都ok了, 说明append完成, 返回true
	DPrintf(dAppend, "T%d: S%d <- S%d receive AppendEntries and reply true.", rf.currentTerm, rf.me, args.LeaderId)
	reply.Term, reply.Success = rf.currentTerm, true
	return
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
			if args.Term < rf.currentTerm || rf.state != CANDIDATE {
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
			//发心跳包通知其他节点自己Leader的身份
			rf.heartbeatTimer.Reset(0 * time.Second)
			// 重置leader特有的matchIndex和nextIndex
			for peer := range rf.peers {
				rf.nextIndex[peer] = rf.log.lastEntry().Index + 1
				rf.matchIndex[peer] = 0
			}
			rf.matchIndex[rf.me] = rf.log.lastEntry().Index
		}
		//就算失败, 也不需要额外做什么, 因为一个任期内只能选举一次, 失败了就等下一次
	}
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
			rf.broadcastEntries(peer)
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
			if rf.log.Entries[matchindex[i]].Term == rf.currentTerm {
				DPrintf(dCommit, "T%d: S%d commit the entries as leader: Entries[%d->%d]", rf.currentTerm, rf.me, rf.commitIndex+1, matchindex[i])
				rf.commitIndex = matchindex[i]
				rf.applyCond.Broadcast()
			}
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) broadcastEntries(peer int) {
	rf.mu.RLock()
	if rf.state != LEADER {
		rf.mu.RUnlock()
		return
	}
	term := rf.currentTerm
	leaderid := rf.me
	preLogEntry := rf.log.Entries[rf.nextIndex[peer]-1] //根据nextIndex找到要发送的下一条的前一个
	var entries []Entry
	leadercommit := rf.commitIndex
	//leader rules: if last log index >= nextIndex, send AE RPC with log entries starting at nextIndex
	if rf.log.lastEntry().Index >= rf.nextIndex[peer] {
		//entries = rf.log.Entries[rf.nextIndex[peer]:]		//NOTE:不能直接赋值, 因为go中切片的赋值是直接共享底层数据的数据, 必须用copy
		entries = make([]Entry, len(rf.log.Entries)-rf.nextIndex[peer])
		copy(entries, rf.log.Entries[rf.nextIndex[peer]:])
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
	rf.mu.RLock()
	if args.Term < rf.currentTerm || rf.state != LEADER {
		DPrintf(dAppend, "T%d: S%d <- S%d received expired rpc reply. [args.Term=%d, currentTerm=%d]", rf.currentTerm, rf.me, peer, args.Term, rf.currentTerm)
		rf.mu.RUnlock()
		return
	}
	rf.mu.RUnlock()
	//如果失败了, 说明对方的term更大, 需要更新状态为follower
	rf.mu.Lock()
	if reply.Success == true {
		//if success, update nextIndex and matchIndex, 如果成功就更新next和match
		rf.nextIndex[peer] = rf.log.lastEntry().Index + 1
		rf.matchIndex[peer] = rf.log.lastEntry().Index
	} else {
		// if failed
		if reply.Term > rf.currentTerm {
			// 如果是收到了更新的term, convert to follower
			DPrintf(dAppend, "T%d: S%d <- S%d received newer term %d from servers", rf.currentTerm, rf.me, peer, reply.Term)
			rf.state = FOLLOWER
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			//rf.electionTimer.Reset(rf.randomElectionTime())
		} else {
			// 剩下就是因为日志不匹配而失败
			// if failed because of log inconsistency, decrement nextIndex and retry
			DPrintf(dAppend, "T%d: S%d <- S%d receive refuse for log inconsistency, decrease nextIndex:[%v->%v]", rf.currentTerm, rf.me, peer, rf.nextIndex[peer], rf.nextIndex[peer]-1)
			rf.nextIndex[peer]--
			// 这里只需要修改nextIndex就行, 下次heartbeat的时候自然会重发, 不需要用一个for循环来确保rpc成功.
		}
	}
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
			rf.electionTimer.Reset(rf.randomElectionTime()) //只要触发了, 就自动重置一次
		case <-rf.heartbeatTimer.C:
			// 发送心跳包计时器超时, 需要发送心跳包, 同样只有leader可以发心跳包
			rf.mu.RLock()
			canBroadHB := rf.state == LEADER
			rf.mu.RUnlock()
			if canBroadHB {
				go rf.broadcastHeartbeat()
			}
			rf.heartbeatTimer.Reset(rf.stableHeartbeatTime())
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
		copy(entries, rf.log.Entries[rf.lastApplied+1:rf.commitIndex+1])
		rf.mu.Unlock()
		// apply是一个写chan的过程, 写chan可能阻塞, 所以这个过程中不要持锁
		for _, e := range entries {
			rf.applyChan <- ApplyMsg{
				CommandValid: true,
				Command:      e.MetaData,
				CommandIndex: e.Index,
			}
		}
		rf.mu.Lock()
		// 注意这里必须记录的commitindex和lastapplied, 因为applych的过程不是阻塞的, 这个过程中rf.commitIndex可能会发生变化
		DPrintf(dApply, "T%d: S%d applied the log: [%d -> %d]", rf.currentTerm, rf.me, lastapplied+1, commitindex)
		rf.lastApplied = commitindex
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
	ms := 600 + (rand.Int63() % 300)
	return time.Duration(ms) * time.Millisecond
}

//
// StableHeartbeatTime
//  @Description: 生成固定的心跳包间隔
//  @receiver rf
//  @return time.Duration
//
func (rf *Raft) stableHeartbeatTime() time.Duration {
	ms := 150
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartbeatTimer = time.NewTimer(rf.stableHeartbeatTime()) //只有当了leader才能初始化这个心跳计时器
	rf.electionTimer = time.NewTimer(rf.randomElectionTime())
	// (2B)
	rf.log = SafeEntryLog{Entries: []Entry{{Index: 0, Term: 0}}} //要求日志index从1开始, 所以这里插入一个空的0日志;
	rf.commitIndex = 0                                           //state: initialized to 0, increases monotonically
	rf.lastApplied = 0                                           //state: initialized to 0, increases monotonically
	rf.nextIndex = make([]int, len(rf.peers))                    //state: initialized to leader last log index + 1
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

	return rf
}
