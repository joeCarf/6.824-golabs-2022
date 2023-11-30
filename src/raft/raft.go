package raft

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

import (
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const HEART_BEAT_TIMEOUT = 120 * time.Millisecond

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

type Status int

const (
	Leader Status = iota
	Follower
	Candidate
)

type LogEntry struct {
	Term    int
	Command interface{}
}

type AppendEntriesRequest struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry // Empty if heartbeat
	LeaderCommit int
}

type AppendEntriesResponse struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntriesRpc(request *AppendEntriesRequest, response *AppendEntriesResponse) {

	if rf.killed() {
		response.Success = false
		response.Term = -1
		return
	}

	if rf.currentTerm > request.Term { // Leader已经落后
		response.Term = rf.currentTerm
		response.Success = false
		return
	}

	rf.mu.Lock()
	rf.currentTerm = request.Term
	rf.timer.Reset(rf.overTime)
	rf.votedFor = request.LeaderId
	rf.status = Follower
	rf.mu.Unlock()

	// resolve heart beat
	if request.Entries == nil {
		DPrintf("[	    func-HeartBeat-reveive-rf(%+v)		] : rf.Term: %v\n", rf.me, rf.currentTerm)
		response.Term = rf.currentTerm
		response.Success = true
		return
	}

	// TODO: default
	response.Term = -1
	response.Success = true

}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	status      Status     // 结点当前的角色
	entries     []LogEntry // log
	currentTerm int        // 当前任期号
	votedFor    int        // 当前任期投票给了谁

	commitIndex int           // 已经提交的最大index号
	lastApplied int           // 提交给statemachine的最大索引号
	timer       *time.Timer   // 定时器
	overTime    time.Duration // 默认超时时间

	// for Leaders
	nextIndex  []int // 记录下一个需要发送到server[i]的log index
	matchIndex []int // 已经被同步到server[i]的log index

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.status == Leader
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
	Term         int
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(request *RequestVoteArgs, response *RequestVoteReply) {
	// Your code here (2A, 2B).

	DPrintf("[	received   func-RequestVote-rf(%+v)		] : rf.term: %v, request:%+v, time: %+v, cur overtime = %+v\n", rf.me, rf.currentTerm, request, getTimeStamp(), rf.overTime)
	if rf.killed() {
		response.Term = -1
		response.VoteGranted = false
		return
	}

	if request.Term < rf.currentTerm {
		response.Term = rf.currentTerm
		response.VoteGranted = false
		return
	}

	currentLogIndex := len(rf.entries) - 1
	currentLogTerm := 0
	if currentLogIndex > 0 {
		currentLogTerm = rf.entries[currentLogIndex].Term
	}

	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	if request.Term > rf.currentTerm {
		rf.status = Follower
		rf.currentTerm = request.Term
		rf.votedFor = -1
	}
	if (rf.votedFor == -1 || rf.votedFor == request.CandidateId) && request.LastLogIndex >= currentLogIndex && request.LastLogTerm >= currentLogTerm { // 可以投票
		rf.votedFor = request.CandidateId
		rf.status = Follower

		rf.currentTerm = request.Term

		response.Term = rf.currentTerm
		response.VoteGranted = true
		DPrintf("[	   func-RequestVote-rf(%+v)		] : rf.voted: %v, time: %+v, cur overtime = %+v\n", rf.me, rf.votedFor, getTimeStamp(), rf.overTime)
		rf.timer.Reset(rf.overTime)
		return
	}

	// TODO: default
	response.Term = rf.currentTerm
	response.VoteGranted = false
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

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesRequest, reply *AppendEntriesResponse) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesRpc", args, reply)
	return ok
}

func getTimeStamp() int64 {
	now := time.Now().UnixNano()
	return now / 1e6
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
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		select {

		case <-rf.timer.C:
			{
				if rf.killed() {
					return
				}
				rf.mu.Lock()
				switch rf.status {
				case Leader:
					rf.timer.Reset(HEART_BEAT_TIMEOUT)

					// send heartbeat
					for i := 0; i < len(rf.peers); i++ {
						if i == rf.me {
							continue
						}
						go func(i int) {
							request := AppendEntriesRequest{
								Term:         rf.currentTerm,
								LeaderId:     rf.me,
								LeaderCommit: rf.commitIndex,

								// Empty below for heartbeat
								Entries:      nil,
								PrevLogIndex: 0,
								PrevLogTerm:  0,
							}
							response := AppendEntriesResponse{}
							sendOk := false
							retryCount := 0
							for !sendOk {
								sendOk = rf.sendAppendEntry(i, &request, &response)
								retryCount++
							}

							rf.mu.Lock()
							if !response.Success && response.Term > rf.currentTerm { // 退为follower
								rf.status = Follower
								rf.timer.Reset(rf.overTime)
								rf.votedFor = -1
								rf.currentTerm = response.Term
							}
							rf.mu.Unlock()
						}(i)

					}

				case Follower:
					rf.status = Candidate
					fallthrough
				case Candidate:

					rf.currentTerm++
					rf.votedFor = rf.me
					voteCount := 1
					var voteCountMu sync.Mutex
					rf.overTime = time.Duration(200+rand.Intn(200)) * time.Millisecond
					rf.timer.Reset(rf.overTime)
					DPrintf("[	    Peer %v become Candidate, Candidate term is %v	, time: %+v] , cur overtime = %+v\n", rf.me, rf.currentTerm, getTimeStamp(), rf.overTime)

					for i := 0; i < len(rf.peers); i++ {
						if i == rf.me {
							continue
						}
						go func(i int) {
							voteRequest := RequestVoteArgs{
								Term:         rf.currentTerm,
								CandidateId:  rf.me,
								LastLogIndex: len(rf.entries) - 1,
								LastLogTerm:  0,
							}
							if len(rf.entries) > 0 {
								lastLogIndex := len(rf.entries) - 1
								voteRequest.LastLogTerm = rf.entries[lastLogIndex].Term
							}
							voteResponse := RequestVoteReply{}
							sendOk := false
							retryCount := 0
							for !sendOk {
								//fmt.Printf("[	request vote for dead  Peer %v, current Peer = %v	] \n", i, rf.me)
								sendOk = rf.sendRequestVote(i, &voteRequest, &voteResponse)
								retryCount++
							}

							if voteResponse.VoteGranted {
								DPrintf("[	 reveive vote from  Peer %v, current Peer = %v, time: %+v] , cur overtime = %+v\n", i, rf.me, getTimeStamp(), rf.overTime)
								voteCountMu.Lock()
								voteCount++
								if voteCount >= (1+len(rf.peers))/2 { // 选票超过半数
									rf.mu.Lock()
									if rf.status == Candidate {
										rf.status = Leader
									}
									rf.mu.Unlock()
								}
								voteCountMu.Unlock()
							} else {
								DPrintf("[	NOT reveive vote from  Peer %v, current Peer = %v	time: %+v] , cur overtime = %+v\n", i, rf.me, getTimeStamp(), rf.overTime)
							}
						}(i)

					}
					DPrintf("[	  current Peer = %v Got %v votes] time: %+v, cur overtime = %+v\n", rf.me, voteCount, getTimeStamp(), rf.overTime)

					if voteCount >= len(rf.peers)/2+1 && rf.status == Candidate { // 超过半数，成为Leader
						//if rf.status == Candidate {
						//	rf.status = Leader
						//}
						rf.nextIndex = make([]int, len(rf.peers))
						for i, _ := range rf.nextIndex {
							rf.nextIndex[i] = len(rf.entries) - 1
						}
						rf.matchIndex = make([]int, len(rf.peers))
						rf.timer.Reset(HEART_BEAT_TIMEOUT)
						DPrintf("peer %v become Leader， status = %v, time: %+v, cur overtime = %+v\n", rf.me, rf.status, getTimeStamp(), rf.overTime)
					}

				}
				rf.mu.Unlock()
			}
		}

	}
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
	rf.currentTerm = 0
	rf.overTime = time.Duration(200+rand.Intn(200)) * time.Millisecond
	rf.timer = time.NewTimer(rf.overTime)
	rf.votedFor = -1
	rf.status = Follower
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.entries = make([]LogEntry, 0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
