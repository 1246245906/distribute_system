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
	//	"bytes"

	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

type RaftState int

type Log struct {
	index      int
	term       int
	logEntries string
}

const TimeoutFixInterval = 500 //ms

const (
	raftStateFollower RaftState = iota
	raftStateCandidate
	raftStateLeader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A zhc, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	curState         RaftState
	curTerm          int
	log              []Log // 从1开始
	commitIndex      int
	lastApplied      int
	votedFor         int
	timeoutTimestamp int64
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.curTerm
	isleader = rf.curState == raftStateLeader
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int // zhc: why not term - 1?
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1. candidate 的lastLogterm  <  自身的Term，不支持
	// 2. 已经投过票切投的不是现在申请的这个，也不支持
	if args.LastLogTerm < rf.curTerm || (rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.VoteGranted = false
		reply.Term = rf.curTerm
		return
	}
	if args.LastLogTerm == rf.curTerm {
		if args.LastLogIndex < rf.log[len(rf.log)-1].index {
			reply.VoteGranted = false
			reply.Term = rf.curTerm
			return
		}
	}
	reply.VoteGranted = true
	reply.Term = rf.curTerm
	// todo votedFor什么时候还原？
	rf.votedFor = args.CandidateId
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) kickOffVote() {
	rf.mu.Lock()
	rf.curTerm += 1
	rf.votedFor = rf.me
	rf.timeoutTimestamp = NewTimestamp()
	rf.curState = raftStateCandidate
	rf.mu.Unlock()

	var votedNums int32 = 0
	var agreedNums int32 = 0
	var args RequestVoteArgs
	args.Term = rf.curTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.log[len(rf.log)-1].index
	args.LastLogTerm = rf.log[len(rf.log)-1].term
	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				var reply RequestVoteReply
				start := time.Now().UnixMilli()
				ok := rf.sendRequestVote(i, &args, &reply) // 最慢什么时候返回呢？先假设比较快就返回了
				// 什么时候返回不重要，如果超时了不要影响raft状态即可
				if ok {
					atomic.AddInt32(&votedNums, 1)
					if reply.VoteGranted {
						atomic.AddInt32(&agreedNums, 1)
					}
				} else {
					end := time.Now().UnixMilli()
					DPrintf("sendRequestVote RPC call faild, cost %dms\n", end-start)
				}
			}(i)
		}
	}
	times := 0
	for {
		// 2n+1 个服务器需要n+1个同意，自己默认同意，所以还需要 (2n+1) / 2个同意
		if atomic.LoadInt32(&agreedNums) >= int32(len(rf.peers)/2) {
			rf.mu.Lock()
			rf.curState = raftStateLeader
			rf.mu.Unlock()
			DPrintf("server id %d become leader!\n", rf.me)
			return
		}
		// 当所有人投完票或者超时了，直接返回
		if (atomic.LoadInt32(&votedNums) >= int32(len(rf.peers)-1)) || (times >= 8) {
			DPrintf("server id %d loss the leader election!\n", rf.me)
			return
		}
		times += 1
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		if rf.curState == raftStateFollower || rf.curState == raftStateCandidate {
			// var voteNums int32 = 0
			if time.Now().UnixNano() > rf.timeoutTimestamp {
				// 发起选举
				// 假设执行kickoffvote时，curstate变成leader该怎么办？
				// 也就是说：正准备执行kickOffVote时，上一轮选举成功了
				// 理论上第二轮选举开始后第一轮选举必须停止，或者说第一轮的
				// 结果不能影响第二轮。所以，添加超时机制，当选举时间超过
				// 800ms后，第一轮选举作废
				DPrintf("server %d kick off vote!\n", rf.me)
				go rf.kickOffVote()
			}
		} else if rf.curState == raftStateLeader {
			// todo
			rf.sendAppendEntries2All()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(100) * time.Millisecond)

	}
}

type AppendEntriesArgs struct {
	Term int
	// LeaderId     int
	// PreLogIndex  int
	// PreLogTerm   int
	// Entries      []Log
	// LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.timeoutTimestamp = NewTimestamp()

	if args.Term < rf.curTerm {
		reply.Term = rf.curTerm
		reply.Success = false
		return
	}
	rf.curTerm = args.Term
	reply.Term = rf.curTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries2All() {
	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				var args AppendEntriesArgs
				args.Term = rf.curTerm
				var reply AppendEntriesReply
				rf.sendAppendEntries(i, &args, &reply)
			}(i)
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.curState = raftStateFollower
	rf.curTerm = 0
	rf.log = append(rf.log, Log{0, 0, "init"})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.timeoutTimestamp = NewTimestamp()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
