package dynamo

import (
	"crypto/md5"
	"encoding/hex"
	"math/big"
	"strconv"
	"sync"
	"sync/atomic"

	"../labrpc"
	"github.com/DistributedClocks/GoVector/govec/vclock"
)

// Question 1: Is intendedNode always assigned the coordinator in static preference list?
// If it can be assigned another coordinator, then how is this used?
// I think this question is relevant for handling network partition, and then node outages

// Otherise, if fixed from static pref list, then how to handle

// Question 2: Does the coordinator node collect responses from all replicas, and
// then reply to client? Yes it does. We do not have all nodes reply to client directly.

// Question 3: Since Dyanmo doesn't provide any isolation guarantee in ACID, nodes
// don't need any locks when reading or writing data, right?  Do muliple clients
// cause any issues in Dynamo?  How to test? Multiple clients are meaningless.

// Question 4: Are transactions to the same coordinator serializable?

// Question 5: Should we make a test case to reorder messages or does that not a concern?

// Note: coordinator does the reconciliation

// Note: concurrent client updates is a useless test case since the last writer will win
// due to having a descendent vector time stamp, which is valid to apply directly in an update.

// However, if dynamo has a node outage / network partition, then we can have "concurrent" updates
// and need to use the Figure 3 case in the paper.

type Context struct {
	// If the intendedNode does not match the expected coordinator, then
	// this is a signal that the temporary replica must store the data in
	// a separate local database (hinted handoff)

	// only useful for puts
	IntendedNode int
	// See paper section 4.3: anytime the client's timestamp metadata does not
	// exactly match dynamo timestamp, the user must reconcile the data later

	// If the most recent state of the cart is unavailable,
	// and a user makes changes to an older version of the cart,
	// that change is still meaningful and should be preserved.
	// But at the same time it shouldn’t supersede the currently
	// unavailable state of the cart, which itself may contain
	// changes that should be preserved.

	// only usefule for gets
	VectorTimestamp vclock.VClock
}

type valueField struct {
	data      int
	timestamp vclock.VClock
}

type Dynamo struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]
	dead  int32               // set by Kill()

	nodeCount int
	replicas  int
	quorumR   int
	quorumW   int

	keyValue map[string]valueField
}

func (rf *Dynamo) findCoordinator(key string) int {
	bi := big.NewInt(0)
	keyBytes := []byte(key)
	output := md5.Sum(keyBytes)
	hexstr := hex.EncodeToString(output[:])
	bi.SetString(hexstr, 16)
	coordNode := new(big.Int)
	nodeCount := big.NewInt(int64(rf.nodeCount))
	coordNode = coordNode.Mod(bi, nodeCount)
	coordNodeInt64 := coordNode.Int64()
	coordNodeInt := int(coordNodeInt64)
	return coordNodeInt
}

// *************************** Client API ***************************

type GetArgs struct {
	Key string //[]byte
}

type GetReply struct {
	Object  []int
	Context Context
}

func (rf *Dynamo) Get(args *GetArgs, reply *GetReply) {
	coordinatorNode := rf.findCoordinator(args.Key)

	if rf.me == coordinatorNode {
		// Current dynamo node is also the correct coordinator for this request
		// cannot put in goroutine since reply is not available synchronously!
		rf.dynamoGet(args, reply)
	} else {
		rf.peers[coordinatorNode].Call("Dynamo.RouteGetToCoordinator", args, reply)
	}
	return
}

type PutArgs struct {
	Key     string //[]byte
	Object  int
	Context Context
}

type PutReply struct {
}

// Description: Client sent the put request to a random dynamo node.  This node must
// identify the correct coordinator and route the request.
func (rf *Dynamo) Put(args *PutArgs, reply *PutReply) {
	DPrintfNew(InfoLevel, "Node: %v received object: args.Object: %v", rf.me, args.Object)
	coordinatorNode := rf.findCoordinator(args.Key)
	DPrintfNew(InfoLevel, "Put request sent from client mapped to coordinator node: %v", coordinatorNode)

	if rf.me == coordinatorNode {
		// Current dynamo node is also the correct coordinator for this request
		go rf.dynamoPut(args, reply)
	} else {
		rf.peers[coordinatorNode].Call("Dynamo.RoutePutToCoordinator", args, reply)
	}

	return
}

// *************************** Dynamo API ***************************

func (rf *Dynamo) RouteGetToCoordinator(args *GetArgs, reply *GetReply) {
	DPrintfNew(InfoLevel, "Reached RouteGetToCoordinator")
	// verify this node is the coorect coordinator
	coordinatorNode := rf.findCoordinator(args.Key)
	if rf.me != coordinatorNode {
		DPrintfNew(ErrorLevel, "Put request routed to incorrect coordinator node: %v", coordinatorNode)
		return
	}

	rf.dynamoGet(args, reply)
	return
}

func (rf *Dynamo) RoutePutToCoordinator(args *PutArgs, reply *PutReply) {
	DPrintfNew(InfoLevel, "Reached RoutePutToCoordinator")
	// verify this node is the coorect coordinator
	coordinatorNode := rf.findCoordinator(args.Key)
	if rf.me != coordinatorNode {
		DPrintfNew(ErrorLevel, "Put request routed to incorrect coordinator node: %v", coordinatorNode)
		return
	}
	go rf.dynamoPut(args, reply)
	return
}

// apply the put request to the coordinator node
func (rf *Dynamo) dynamoGet(args *GetArgs, reply *GetReply) {
	// To do: launch new go routine where coordinator gets data from repliicas

	reply.Object = make([]int, 0) //make([]int, 1)
	reply.Object = append(reply.Object, rf.keyValue[args.Key].data)
	DPrintfNew(InfoLevel, "dynamoPut replying with object: %v", reply.Object)
	reply.Context.VectorTimestamp = rf.keyValue[args.Key].timestamp
}

// apply the put request to the coordinator node
func (rf *Dynamo) dynamoPut(args *PutArgs, reply *PutReply) {
	// add this data to dynamo data
	value, exists := rf.keyValue[args.Key]
	if exists {

	} else {
		value = valueField{}
		value.data = args.Object
		value.timestamp = vclock.New()
		meString := strconv.Itoa(rf.me)
		value.timestamp.Set(meString, 1)
		rf.keyValue[args.Key] = value
	}

	// second, forward the request to the other replicas
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
//
func (rf *Dynamo) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

	// To do: The tester calls your Raft's rf.Kill() when it is permanently shutting down an instance.
	// You can check whether Kill() has been called using rf.killed(). You may want to do this in
	// all loops, to avoid having dead Raft instances print confusing messages.
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
func Make(peers []*labrpc.ClientEnd, me int, replicaCount int, quorumR int, quorumW int) *Dynamo {
	rf := &Dynamo{}
	rf.peers = peers
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.nodeCount = len(rf.peers) - 1
	rf.replicas = replicaCount
	rf.quorumR = quorumR
	rf.quorumW = quorumW

	rf.keyValue = make(map[string]valueField)

	// rf.nextIndex = make([]int, len(rf.peers))
	// rf.matchIndex = make([]int, len(rf.peers))
	// rf.lastApplied = -1
	// rf.currentTerm = 0
	// rf.votedFor = -1
	// rf.commitIndex = -1
	// rf.selectedLeader = false

	// To do: Modify Make() to create a background goroutine that will kick off leader election periodically
	// by sending out RequestVote RPCs when it hasn't heard from another peer for a while. This way a peer
	// will learn who is the leader, if there is already a leader, or become the leader itself.

	// rand.Seed(int64(rf.me))
	// rf.randMissedHeartbeats = uint((rand.Uint32() % 10) + uint32(6))
	// go leaderElectionMonitoring(rf)

	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())
	// if len(rf.log) > 0 {
	// 	rf.lastApplied = len(rf.log) - 1
	// }

	return rf
}
