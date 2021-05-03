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
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// To do: You'll also need to define a struct to hold information about each log entry.
//
type LogEntry struct {
	// Your data here (2A).

	Cmd  interface{} // each entry contains command for state machine
	Term int         // term when entry was received by leader (first index is 1)
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
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// new custom vars added by us!
	recvdAppendEntriesFlag bool          // set in append entries, clear in timeout monitor background routine
	heartbeatCount         uint          //count number of consecutive missed heartbeats. Eventually triggers new election
	randMissedHeartbeats   uint          // number of missed heartbeats for this peer to trigger election (randomly configured for each peer)
	selectedLeader         bool          // is this peer elected leader (true or false)
	applyCh                chan ApplyMsg // copied from Make()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.selectedLeader
	rf.mu.Unlock()
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	e.Encode(rf.commitIndex)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var commitIndex int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&commitIndex) != nil {
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log

		rf.commitIndex = commitIndex
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
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
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()

	// Current terms are exchanged whenever servers communicate; if one server’s
	// current term is smaller than the other’s, then it updates its current term to the larger value.
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.selectedLeader = false
		rf.votedFor = -1
	}

	// 1. Reply false if term < currentTerm (§5.1) & send term for candidate to update itself
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else {
		// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as
		// receiver’s log, grant vote (§5.2, §5.4)
		if (rf.votedFor == -1) || (rf.votedFor == args.CandidateID) {
			// Check candidate log
			if rf.lastApplied == -1 {
				reply.VoteGranted = true
				reply.Term = rf.currentTerm
				rf.votedFor = args.CandidateID
				rf.heartbeatCount = 0
				rf.persist()
				rf.mu.Unlock()
				return
			}
			if args.LastLogTerm < rf.log[rf.lastApplied].Term {
				// last log entry terms don't match -> not up to date
				reply.VoteGranted = false
				reply.Term = rf.currentTerm
				rf.persist()
				rf.mu.Unlock()
				return
			}

			if args.LastLogTerm == rf.log[rf.lastApplied].Term {
				if args.LastLogIndex < rf.lastApplied {
					// candidates log has fewer entries as this node
					reply.VoteGranted = false
					reply.Term = rf.currentTerm
					rf.persist()
					rf.mu.Unlock()
					return
				}
			}
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.heartbeatCount = 0
			rf.votedFor = args.CandidateID
		}
	}

	rf.persist()
	rf.mu.Unlock()
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

//
// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	// Your data here (2A).
	Term         int        // leader’s term
	LeaderID     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int  // accelerate backtracking
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = -2
		rf.persist()
		rf.mu.Unlock()
		return
	}

	// AppendEntries RPC handler method that resets the election timeout
	// so that other servers don't step forward as leaders when one has already been elected.
	rf.heartbeatCount = 0

	// // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.selectedLeader = false
		rf.votedFor = -1
	}

	// base case follower log is empty then add whatever leader says to add
	if args.PrevLogIndex == -1 {
		// only if valid entry to add to log
		if len(args.Entries) == 0 {
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.ConflictIndex = -2
			rf.persist()
			rf.mu.Unlock()
			return
		}
		rf.log = rf.log[0:0]
		rf.log = append(rf.log, args.Entries...)
		rf.lastApplied = len(args.Entries) - 1
		reply.Success = true
		reply.Term = rf.currentTerm
		rf.persist()
		rf.mu.Unlock()
		return
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if rf.lastApplied >= args.PrevLogIndex {
		// if term does not match PrevLogTerm
		if rf.lastApplied > args.PrevLogIndex {
			rf.log = rf.log[0 : args.PrevLogIndex+1]
			rf.lastApplied = args.PrevLogIndex
		}
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.ConflictIndex = -2
			for index := range rf.log {
				if rf.log[index].Term == rf.log[args.PrevLogIndex].Term {
					reply.ConflictIndex = index
					break
				}
			}
			if args.PrevLogIndex == reply.ConflictIndex {
				reply.ConflictIndex = -2
			}
			rf.persist()
			rf.mu.Unlock()
			return
		}
	}

	// lastApplied < args.PrevLogTerm, so index does not have matching terms
	if rf.lastApplied < args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = rf.lastApplied
		rf.persist()
		rf.mu.Unlock()
		return
	}

	// terms of lastApplied & PrevLogIndex match at this point, delete all entries that follow it
	rf.log = rf.log[0 : args.PrevLogIndex+1]

	// append any new entries not already in log
	rf.log = append(rf.log, args.Entries...)
	rf.lastApplied = len(rf.log) - 1
	reply.Success = true
	reply.Term = rf.currentTerm

	if args.LeaderCommit > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		if args.LeaderCommit < rf.lastApplied {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.lastApplied
		}

		amountToSend := rf.commitIndex - oldCommitIndex
		for i := oldCommitIndex + 1; i < oldCommitIndex+1+amountToSend; i++ {
			// send this follower's updates to client
			var myApplyMsg ApplyMsg
			myApplyMsg.CommandValid = true
			myApplyMsg.Command = rf.log[i].Cmd
			myApplyMsg.CommandIndex = i + 1
			rf.applyCh <- myApplyMsg
		}

	}
	rf.persist()
	rf.mu.Unlock()
	return

	// To do: To implement heartbeats, define an AppendEntries RPC struct (though
	// you may not need all the arguments yet), and have the leader send them out periodically.

	// To do: Write an AppendEntries RPC handler method that resets the election timeout
	// so that other servers don't step forward as leaders when one has already been elected.

	// To do: Make sure the election timeouts in different peers don't always fire at the same
	// time, or else all peers will vote only for themselves and no one will become the leader.

	// To do: The tester requires that the leader send heartbeat RPCs no more than ten times per second.

	// To do: The tester requires your Raft to elect a new leader within five seconds of the failure of the old
	// leader (if a majority of peers can still communicate). Remember, however, that leader election may require
	// multiple rounds in case of a split vote (which can happen if packets are lost or if candidates unluckily
	// choose the same random backoff times). You must pick election timeouts (and thus heartbeat intervals)
	// that are short enough that it's very likely that an election will complete in less than five seconds even
	// if it requires multiple rounds.

	// To do: The paper's Section 5.2 mentions election timeouts in the range of 150 to 300 milliseconds. Such a
	// range only makes sense if the leader sends heartbeats considerably more often than once per 150
	// milliseconds. Because the tester limits you to 10 heartbeats per second, you will have to use an election
	// timeout larger than the paper's 150 to 300 milliseconds, but not too large, because then you may fail to
	// elect a leader within five seconds.

	// To do: You may find Go's rand useful.

	// To do: You'll need to write code that takes actions periodically or after delays in time. The easiest
	// way to do this is to create a goroutine with a loop that calls time.Sleep(). Don't use Go's time.Timer
	// or time.Ticker, which are difficult to use correctly.

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
	if rf.selectedLeader == false {
		isLeader = false
	} else {
		var addEntry LogEntry
		addEntry.Cmd = command
		addEntry.Term = rf.currentTerm
		rf.mu.Lock()
		rf.log = append(rf.log, addEntry)
		rf.lastApplied += 1
		index = rf.lastApplied + 1
		term = rf.currentTerm
		rf.persist()
		rf.mu.Unlock()
		// leader to send out cmd during heartbeat
	}

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

	// To do: The tester calls your Raft's rf.Kill() when it is permanently shutting down an instance.
	// You can check whether Kill() has been called using rf.killed(). You may want to do this in
	// all loops, to avoid having dead Raft instances print confusing messages.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func leaderSendAppendRoutine(rf *Raft) {
	rf.mu.Lock()
	rf.heartbeatCount = 0
	var reqArgs []AppendEntriesArgs
	var voteReply []AppendEntriesReply
	var timeoutArray []bool

	reqArgs = make([]AppendEntriesArgs, len(rf.peers))
	voteReply = make([]AppendEntriesReply, len(rf.peers))
	timeoutArray = make([]bool, len(rf.peers))

	for index := 0; index < len(rf.peers); index++ {
		if index == rf.me {
			continue
		} else {
			// init values here
			reqArgs[index].Term = rf.currentTerm
			reqArgs[index].LeaderID = rf.me
			reqArgs[index].PrevLogIndex = rf.nextIndex[index] - 1
			if reqArgs[index].PrevLogIndex == -1 {
				reqArgs[index].PrevLogTerm = 0
			} else {
				reqArgs[index].PrevLogTerm = rf.log[rf.nextIndex[index]-1].Term
			}
			reqArgs[index].LeaderCommit = rf.commitIndex
			reqArgs[index].Entries = rf.log[reqArgs[index].PrevLogIndex+1 : rf.lastApplied+1]

			sendAppendEntriesParallel := func(rf *Raft, index int, args *AppendEntriesArgs, reply *AppendEntriesReply, timeout *bool) {
				*timeout = rf.sendAppendEntries(index, args, reply)
			}
			go sendAppendEntriesParallel(rf, index, &(reqArgs[index]), &(voteReply[index]), &(timeoutArray[index]))
		}
	}

	rf.mu.Unlock()
	time.Sleep(60 * time.Millisecond)
	rf.mu.Lock()

	for index := 0; index < len(rf.peers); index++ {
		if index == rf.me {
			rf.matchIndex[index] = rf.lastApplied
			continue
		}
		if timeoutArray[index] == false {
			continue // this node has a network partition - don't count it
		}

		// step down and convert to follower
		if voteReply[index].Term > rf.currentTerm {
			rf.currentTerm = voteReply[index].Term
			rf.selectedLeader = false
			rf.persist()
			rf.mu.Unlock()
			return
		}

		if voteReply[index].Term != reqArgs[index].Term {
			continue
		}

		if voteReply[index].Success == true {
			// update nextIndex and matchIndex for follower
			rf.nextIndex[index] += len(reqArgs[index].Entries)
			rf.matchIndex[index] = rf.nextIndex[index] - 1
		} else {
			if voteReply[index].ConflictIndex != -2 {
				rf.nextIndex[index] = voteReply[index].ConflictIndex + 1
			} else if rf.nextIndex[index] > 0 {
				rf.nextIndex[index]--
			}
		}
	}

	// calculate new commitIndex using median of matchIndex values
	matchIndexCopy := make([]int, len(rf.matchIndex))
	copy(matchIndexCopy, rf.matchIndex)
	sort.Ints(matchIndexCopy)
	medianIndex := len(rf.peers) / 2
	potentialCommitIndex := matchIndexCopy[medianIndex]
	if (len(rf.log) > 0) && (potentialCommitIndex > -1) && (rf.log[potentialCommitIndex].Term == rf.currentTerm) {
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = potentialCommitIndex
		amountToSend := rf.commitIndex - oldCommitIndex
		for i := oldCommitIndex + 1; i < oldCommitIndex+1+amountToSend; i++ {
			var myApplyMsg ApplyMsg
			myApplyMsg.CommandValid = true
			myApplyMsg.Command = rf.log[i].Cmd
			myApplyMsg.CommandIndex = i + 1
			rf.applyCh <- myApplyMsg
		}
	}

	rf.persist()
	rf.mu.Unlock()
}

func candidateStartsElection(rf *Raft) {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.heartbeatCount = 0

	var reqArgs RequestVoteArgs
	var voteReply []RequestVoteReply
	var timeoutArray []bool

	reqArgs.Term = rf.currentTerm
	reqArgs.CandidateID = rf.me
	reqArgs.LastLogIndex = rf.lastApplied
	if rf.lastApplied < 0 {
		reqArgs.LastLogTerm = -1
	} else {
		reqArgs.LastLogTerm = rf.log[rf.lastApplied].Term
	}

	voteReply = make([]RequestVoteReply, len(rf.peers))
	timeoutArray = make([]bool, len(rf.peers))

	for index := 0; index < len(rf.peers); index++ {
		if index == rf.me {
			continue
		} else {
			sendRequestVoteParallel := func(rf *Raft, index int, args *RequestVoteArgs, reply *RequestVoteReply, timeout *bool) {
				*timeout = rf.sendRequestVote(index, args, reply)
			}
			go sendRequestVoteParallel(rf, index, &reqArgs, &(voteReply[index]), &(timeoutArray[index]))
		}
	}

	rf.persist()
	rf.mu.Unlock()
	time.Sleep(60 * time.Millisecond)
	rf.mu.Lock()
	var voteCount int = 1 //vote for self

	for index := 0; index < len(rf.peers); index++ {
		if index == rf.me {
			continue
		}
		if timeoutArray[index] == false {
			continue // this node has a network partition - don't count it
		}

		// candidate must update term number
		if voteReply[index].Term > rf.currentTerm {
			rf.selectedLeader = false
			rf.currentTerm = voteReply[index].Term
			rf.persist()
			rf.mu.Unlock()
			return
		}

		// if voteReply[index].Term != reqArgs.Term {
		// 	continue
		// }

		if voteReply[index].VoteGranted {
			voteCount++
		}
	}

	if voteCount > len(rf.peers)/2 {
		rf.selectedLeader = true
		for i := range rf.nextIndex {
			rf.nextIndex[i] = rf.lastApplied + 1
		}
		for i := range rf.matchIndex {
			rf.matchIndex[i] = -1 //0
		}
		// go leaderSendAppendRoutine(rf)
	} else {
		// rf.randMissedHeartbeats = uint((rand.Uint32() % 20) + uint32(6))
		rf.randMissedHeartbeats = uint((rand.Uint32() % 12) + uint32(6))
	}
	rf.persist()
	rf.mu.Unlock()
}

// background goroutine that kicks off leader election periodically
// by sending out RequestVote RPCs when it hasn't heard from another peer for a while. This way a peer
// will learn who is the leader, if there is already a leader, or become the leader itself.
func leaderElectionMonitoring(rf *Raft) {
	for {
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			break
		}
		// for each peer reset the timeout in AppendEntries
		if rf.selectedLeader { // if leader
			// send out appendEntries heartbeat
			go leaderSendAppendRoutine(rf)
		} else {
			// peer is follower
			// rf.mu.Lock()
			rf.heartbeatCount++
			if rf.heartbeatCount >= rf.randMissedHeartbeats {
				go candidateStartsElection(rf)
			}
			// rf.mu.Unlock()
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.lastApplied = -1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.selectedLeader = false

	// To do: Modify Make() to create a background goroutine that will kick off leader election periodically
	// by sending out RequestVote RPCs when it hasn't heard from another peer for a while. This way a peer
	// will learn who is the leader, if there is already a leader, or become the leader itself.

	rand.Seed(int64(rf.me))
	// rf.randMissedHeartbeats = uint((rand.Uint32() % 20) + uint32(6))
	rf.randMissedHeartbeats = uint((rand.Uint32() % 10) + uint32(6))
	go leaderElectionMonitoring(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if len(rf.log) > 0 {
		rf.lastApplied = len(rf.log) - 1
	}

	return rf
}
