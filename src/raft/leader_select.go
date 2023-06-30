package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

const TimeoutFixInterval = 500  //ms
const RandomRetryInterval = 500 // ms

func (rf *Raft) updateTimer() {
	rf.timeoutTimestamp = time.Now().UnixNano() + Ms2Nano(TimeoutFixInterval+(rand.Int63()%RandomRetryInterval))
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
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
	if args.Term < rf.curTerm || (args.Term == rf.curTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.VoteGranted = false
		reply.Term = rf.curTerm
		return
	}
	LastLogTerm := rf.log[len(rf.log)-1].term
	LastLogIndex := rf.log[len(rf.log)-1].index
	if args.LastLogTerm < LastLogTerm || (args.LastLogTerm == LastLogTerm && args.LastLogIndex < LastLogIndex) {
		reply.VoteGranted = false
		reply.Term = rf.curTerm
		return
	}
	DPrintf("server %d voting, vote %d, term is %d\n", rf.me, args.CandidateId, args.Term)
	rf.votedFor = args.CandidateId
	rf.curTerm = args.Term
	rf.curState = raftStateFollower
	rf.updateTimer()
	reply.VoteGranted = true
	reply.Term = rf.curTerm
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

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.curTerm += 1
	rf.votedFor = rf.me
	rf.updateTimer()
	rf.curState = raftStateCandidate
	rf.mu.Unlock()
	DPrintf("server %d time out, kick off vote, term is %d\n", rf.me, rf.curTerm)

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
				// start := time.Now().UnixMilli()
				ok := rf.sendRequestVote(i, &args, &reply) // 最慢什么时候返回呢？先假设比较快就返回了
				// 什么时候返回不重要，如果超时了不要影响raft状态即可
				if ok {
					atomic.AddInt32(&votedNums, 1)
					if reply.VoteGranted {
						atomic.AddInt32(&agreedNums, 1)
					}
				} else {
					// end := time.Now().UnixMilli()
				}
			}(i)
		}
	}
	times := 0
	for {
		// 2n+1 个服务器需要n+1个同意，自己默认同意，所以还需要 (2n+1) / 2个同意
		if atomic.LoadInt32(&agreedNums) >= int32(len(rf.peers)/2) {
			rf.mu.Lock()
			// 有没有可能等待期间变成follower？ 保险起见先加上
			if rf.curState == raftStateCandidate {
				rf.curState = raftStateLeader
			}
			rf.mu.Unlock()
			DPrintf("server id %d become leader! term is %d\n", rf.me, rf.curTerm)
			if rf.curState == raftStateLeader {
				rf.sendAppendEntries2All()
			}
			return
		}
		// 当所有人投完票或者超时了，直接返回
		if (atomic.LoadInt32(&votedNums) >= int32(len(rf.peers)-1)) || (times >= 3) {
			return
		}
		times += 1
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}
