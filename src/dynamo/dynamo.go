package dynamo

import (
	"crypto/md5"
	"encoding/hex"
	"math/big"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

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

type ValueField struct {
	Data      int
	Timestamp vclock.VClock
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

	keyValue                 map[string]ValueField
	prefList                 map[int][]int
	failureDetectionPeriodms int
	RPCtimeout               int
	ShiVizVClock             vclock.VClock
}

func contains(value []int, token int) bool {
	for _, v := range value {
		if v == token {
			return true
		}
	}
	return false
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
	expectedCoordNodeInt := int(coordNodeInt64)
	actualCoordNode := expectedCoordNodeInt
	// check who owns this node's keys in the preference list

	aliveList := rf.getListLiveNodes()
	for i := 0; i < len(aliveList); i++ {
		result := contains(rf.prefList[aliveList[i]], expectedCoordNodeInt)
		if result {
			actualCoordNode = aliveList[i]
			break
		}
	}
	DPrintfNew(InfoLevel, "route request to coord node: %v", actualCoordNode)
	return actualCoordNode
}

// *************************** Client API ***************************

type GetArgs struct {
	Key    string //[]byte
	VClock vclock.VClock
}

type GetReply struct {
	Object  []int
	Context Context
	VClock  vclock.VClock
}

func (rf *Dynamo) Get(args *GetArgs, reply *GetReply) {
	// rf.mu.Lock()
	// Merge and Tick
	rf.ShiVizVClock.Merge(args.VClock)
	rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
	// Add to log
	DPrintfNew(ShiVizLevel, "event: GetHandler\thost: %v\tclock:%v", rf.me, rf.ShiVizVClock.ReturnVCString())
	coordinatorNode := rf.findCoordinator(args.Key)
	if rf.me == coordinatorNode {
		// Current dynamo node is also the correct coordinator for this request
		// cannot put in goroutine since reply is not available synchronously!

		rf.dynamoGet(args, reply)
	} else {
		// Tick Node
		rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
		// Add to args
		args.VClock = rf.ShiVizVClock
		// Add to log
		DPrintfNew(ShiVizLevel, "event: RouteGetToCoordinator\thost: %v\tclock:%v", rf.me, args.VClock.ReturnVCString())

		ack := rf.peers[coordinatorNode].Call("Dynamo.RouteGetToCoordinator", args, reply, -1)

		if ack {
			// Merge and Tick
			rf.ShiVizVClock.Merge(reply.VClock)
			rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
			// Add to log
			DPrintfNew(ShiVizLevel, "event: RouteGetToCoordinatorAck\thost: %v\tclock:%v", rf.me, rf.ShiVizVClock.ReturnVCString())
		}

	}
	// rf.mu.Unlock()
	reply.VClock = rf.ShiVizVClock
	return
}

type PutArgs struct {
	Key     string //[]byte
	Object  int
	Context Context
	VClock  vclock.VClock
}

type PutReply struct {
	VClock vclock.VClock
}

// Description: Client sent the put request to a random dynamo node.  This node must
// identify the correct coordinator and route the request.
func (rf *Dynamo) Put(args *PutArgs, reply *PutReply) {
	// rf.mu.Lock()

	// Merge and Tick
	rf.ShiVizVClock.Merge(args.VClock)
	rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
	// Add to log
	DPrintfNew(ShiVizLevel, "event: PutHandler\thost: %v\tclock:%v", rf.me, rf.ShiVizVClock.ReturnVCString())

	DPrintfNew(InfoLevel, "Node: %v received object: args.Object: %v", rf.me, args.Object)
	coordinatorNode := rf.findCoordinator(args.Key)
	DPrintfNew(InfoLevel, "Put request sent from client mapped to coordinator node: %v", coordinatorNode)
	if rf.me == coordinatorNode {
		// Current dynamo node is also the correct coordinator for this request
		//go rf.dynamoPut(args, reply)
		rf.dynamoPut(args, reply)
	} else {
		// Tick Node
		rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
		// Add to args
		args.VClock = rf.ShiVizVClock
		// Add to log
		DPrintfNew(ShiVizLevel, "event: RoutePutToCoordinator\thost: %v\tclock:%v", rf.me, args.VClock.ReturnVCString())
		ack := rf.peers[coordinatorNode].Call("Dynamo.RoutePutToCoordinator", args, reply, -1)

		if ack {
			// Merge and Tick
			rf.ShiVizVClock.Merge(reply.VClock)
			rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
			// Add to log
			DPrintfNew(ShiVizLevel, "event: RoutePutToCoordinatorAck\thost: %v\tclock:%v", rf.me, rf.ShiVizVClock.ReturnVCString())
		}
	}
	// rf.mu.Unlock()

	reply.VClock = rf.ShiVizVClock
	return
}

// *************************** Dynamo API ***************************

func (rf *Dynamo) RouteGetToCoordinator(args *GetArgs, reply *GetReply) {

	// Merge and Tick
	rf.ShiVizVClock.Merge(args.VClock)
	rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
	// Add to log
	DPrintfNew(ShiVizLevel, "event: RouteGetToCoordinatorHandler\thost: %v\tclock:%v", rf.me, rf.ShiVizVClock.ReturnVCString())

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

	// Merge and Tick
	rf.ShiVizVClock.Merge(args.VClock)
	rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
	// Add to log
	DPrintfNew(ShiVizLevel, "event: RoutePutToCoordinatorHandler\thost: %v\tclock:%v", rf.me, rf.ShiVizVClock.ReturnVCString())

	DPrintfNew(InfoLevel, "Reached RoutePutToCoordinator")
	// verify this node is the coorect coordinator
	coordinatorNode := rf.findCoordinator(args.Key)
	if rf.me != coordinatorNode {
		DPrintfNew(ErrorLevel, "Put request routed to incorrect coordinator node: %v", coordinatorNode)
		return
	}
	rf.dynamoPut(args, reply)
	reply.VClock = rf.ShiVizVClock
	return
}

type DynamoGetReply struct {
	NodeID int
	Object ValueField
	VClock vclock.VClock
}

func (rf *Dynamo) DynamoGetObject(args *GetArgs, dynamoReply *DynamoGetReply) {

	// Merge and Tick
	rf.ShiVizVClock.Merge(args.VClock)
	rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
	// Add to log
	DPrintfNew(ShiVizLevel, "event: GetObjectHandler\thost: %v\tclock:%v", rf.me, rf.ShiVizVClock.ReturnVCString())

	dynamoReply.NodeID = rf.me
	dynamoReply.Object = rf.keyValue[args.Key]
	dynamoReply.VClock = rf.ShiVizVClock.Copy()
}

func (rf *Dynamo) requestReplicaData(ackChan chan int, index int, intendedNode int, args *GetArgs, dynamoReply *DynamoGetReply) {
	DPrintfNew(InfoLevel, "Sending replica: %v get request", intendedNode)
	rf.mu.Lock()
	// Tick Node
	rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
	// Add to args
	args.VClock = rf.ShiVizVClock.Copy()
	// Add to log
	DPrintfNew(ShiVizLevel, "event: GetObject\thost: %v\tclock:%v", rf.me, args.VClock.ReturnVCString())
	rf.mu.Unlock()

	ack := rf.peers[intendedNode].Call("Dynamo.DynamoGetObject", args, dynamoReply, -1) // Not sure
	if ack == true {
		// return index that responded
		rf.mu.Lock()
		// Merge and Tick
		rf.ShiVizVClock.Merge(dynamoReply.VClock)
		rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
		// Add to log
		DPrintfNew(ShiVizLevel, "event: DynamoGetObjectAck\thost: %v\tclock:%v", rf.me, rf.ShiVizVClock.ReturnVCString())

		ackChan <- index
		rf.mu.Unlock()
	} else {
		// this node did not reply and can print timeout msg
	}
}

// apply the put request to the coordinator node
func (rf *Dynamo) dynamoGet(args *GetArgs, reply *GetReply) {
	// To do: launch new go routine where coordinator gets data from repliicas
	DPrintfNew(InfoLevel, "Coordinator Node: %v is handling get request", rf.me)
	readCount := 1
	tempObject := make([]int, 0)
	tempObject = append(tempObject, rf.keyValue[args.Key].Data)
	tempVectorTimestamp := rf.keyValue[args.Key].Timestamp

	if readCount < rf.quorumR {
		replicaGetArgs := GetArgs{args.Key, args.VClock.Copy()}
		// Waiting for a subset of go routines to complete
		// Use https://stackoverflow.com/questions/52227954/waitgroup-on-subset-of-go-routines

		// buffered channel up to read quorum - 1 returns
		ackChan := make(chan int, rf.quorumR-1)
		dynamoReply := make([]DynamoGetReply, rf.replicas-1)
		for i := 0; i < rf.replicas-1; i++ {
			// To do: update to use the real-time preference list

			// To do: update to use the real-time preference list
			// get a list of nodes which are alive
			aliveList := rf.getListLiveNodes()
			var myIndexAliveList int
			if len(aliveList) < rf.quorumR {
				// Not enough replicas to do get request!
			} else {
				// send the updates to the correct nodes
				for i := 0; i < len(aliveList); i++ {
					if aliveList[i] == rf.me {
						myIndexAliveList = i
						break
					}
				}
			}
			intendedNodeIndex := (myIndexAliveList + i + 1) % len(aliveList)
			intendedNode := aliveList[intendedNodeIndex] //(rf.me + i + 1) % rf.nodeCount

			// intendedNode := (rf.me + i + 1) % rf.nodeCount
			go rf.requestReplicaData(ackChan, i, intendedNode, &replicaGetArgs, &dynamoReply[i])
		}
		for responseCount := 0; responseCount < rf.quorumR-1; responseCount++ {
			index := <-ackChan
			reply.VClock = rf.ShiVizVClock
			DPrintfNew(InfoLevel, "dynamoGet received reply from replica: %v with value: %v", dynamoReply[index].NodeID, dynamoReply[index].Object)
			if dynamoReply[index].Object.Data != rf.keyValue[args.Key].Data {
				tempObject = append(tempObject, dynamoReply[index].Object.Data)
			}
		}
	}

	reply.Object = tempObject
	reply.Context.VectorTimestamp = tempVectorTimestamp
	// time.Sleep(10 * time.Second)
	return
}

type DynamoPutReply struct {
	NodeID int
	VClock vclock.VClock
}

type DynamoPutArgs struct {
	Key    string
	Object ValueField
	VClock vclock.VClock
}

func (rf *Dynamo) DynamoPutObject(dynamoArgs *DynamoPutArgs, dynamoReply *DynamoPutReply) {
	dynamoReply.NodeID = rf.me
	// To do: check vector timestamps before writing it to know if need to preserve the data

	// Merge and Tick
	rf.ShiVizVClock.Merge(dynamoArgs.VClock)
	rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
	// Add to log
	DPrintfNew(ShiVizLevel, "event: DynamoPutObjectHandler\thost: %v\tclock:%v", rf.me, rf.ShiVizVClock.ReturnVCString())

	rf.keyValue[dynamoArgs.Key] = dynamoArgs.Object
	dynamoReply.VClock = rf.ShiVizVClock.Copy()
}

func (rf *Dynamo) sendReplicaData(ackChan chan int, index int, intendedNode int, args *DynamoPutArgs, dynamoReply *DynamoPutReply) {
	DPrintfNew(InfoLevel, "Sending replica: %v put request", intendedNode)
	rf.mu.Lock()
	// Tick Node
	rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
	// Add to args
	args.VClock = rf.ShiVizVClock.Copy()
	// Add to log
	DPrintfNew(ShiVizLevel, "event: DynamoPutObject\thost: %v\tclock:%v", rf.me, args.VClock.ReturnVCString())
	rf.mu.Unlock()

	ack := rf.peers[intendedNode].Call("Dynamo.DynamoPutObject", args, dynamoReply, -1) // Not sure
	// ack := false
	if ack == true {
		// return index that responded
		rf.mu.Lock()
		// Merge and Tick
		rf.ShiVizVClock.Merge(dynamoReply.VClock)
		rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
		// Add to log
		DPrintfNew(ShiVizLevel, "event: DynamoPutObjectAck\thost: %v\tclock:%v", rf.me, rf.ShiVizVClock.ReturnVCString())

		ackChan <- index
		rf.mu.Unlock()
	} else {
		// this node did not reply and can print timeout msg
	}
}

// apply the put request to the coordinator node
func (rf *Dynamo) dynamoPut(args *PutArgs, reply *PutReply) {
	// add this data to the current dynamo node
	DPrintfNew(InfoLevel, "Coordinator Node: %v is handling put request", rf.me)
	value, exists := rf.keyValue[args.Key]
	if exists {
		// to do: handle case of updating existing key

	} else {
		value = ValueField{}
		value.Data = args.Object
		value.Timestamp = vclock.New()
		meString := strconv.Itoa(rf.me)
		value.Timestamp.Set(meString, 1)
		rf.keyValue[args.Key] = value

		rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
		// Add to log
		DPrintfNew(ShiVizLevel, "event: DynamoPutObjectSelf\thost: %v\tclock:%v", rf.me, rf.ShiVizVClock.ReturnVCString())
	}

	writeCount := 1
	if writeCount < rf.quorumW {
		// Waiting for a subset of go routines to complete
		// Use https://stackoverflow.com/questions/52227954/waitgroup-on-subset-of-go-routines

		// buffered channel up to read quorum - 1 returns
		ackChan := make(chan int, rf.quorumW-1)
		dynamoReply := make([]DynamoPutReply, rf.replicas-1)
		for i := 0; i < rf.replicas-1; i++ {
			updatedArgs := DynamoPutArgs{}
			updatedArgs.Key = args.Key
			updatedArgs.Object.Data = value.Data
			updatedArgs.Object.Timestamp = value.Timestamp.Copy()
			// To do: update to use the real-time preference list
			// get a list of nodes which are alive
			aliveList := rf.getListLiveNodes()
			var myIndexAliveList int
			if len(aliveList) < rf.quorumW {
				// Not enough replicas to do put request!
			} else {
				// send the updates to the correct nodes
				for i := 0; i < len(aliveList); i++ {
					if aliveList[i] == rf.me {
						myIndexAliveList = i
						break
					}
				}
			}
			intendedNodeIndex := (myIndexAliveList + i + 1) % len(aliveList)
			intendedNode := aliveList[intendedNodeIndex] //(rf.me + i + 1) % rf.nodeCount
			go rf.sendReplicaData(ackChan, i, intendedNode, &updatedArgs, &dynamoReply[i])
		}
		for responseCount := 0; responseCount < rf.quorumW-1; responseCount++ {
			index := <-ackChan
			reply.VClock = rf.ShiVizVClock
			DPrintfNew(InfoLevel, "dynamoPut received reply from replica: %v", dynamoReply[index].NodeID)
		}
	}

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

func (rf *Dynamo) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

type DynamoPingReply struct {
}

type DynamoPingArgs struct {
}

func (rf *Dynamo) DynamoPing(dynamoArgs *DynamoPingArgs, dynamoReply *DynamoPingReply) {
	return
}

type DynamoUpdatePrefReply struct {
}

type DynamoUpdatePrefArgs struct {
	UpdatedPrefList map[int][]int
	VClock          vclock.VClock
}

func (rf *Dynamo) DynamoUpdatePrefList(dynamoArgs *DynamoUpdatePrefArgs, dynamoReply *DynamoUpdatePrefReply) {
	// add the updates to our preference list
	DPrintfNew(InfoLevel, "Node %v notified of updated prefList (before): %v", rf.me, rf.prefList)
	for node, value := range dynamoArgs.UpdatedPrefList {
		tmp := make([]int, len(value))
		copy(tmp, value)
		// rf.mu.Lock()
		rf.prefList[node] = tmp
		// rf.mu.Unlock()
	}
	DPrintfNew(InfoLevel, "Node %v prefList (after): %v", rf.me, rf.prefList)
	return
}

func (rf *Dynamo) getListLiveNodes() []int {
	// get a list of nodes which are alive
	aliveList := []int{}
	for node, valueList := range rf.prefList {
		flagNodeIsDown := false
		if len(valueList) == 0 {
			flagNodeIsDown = true
		}
		if !flagNodeIsDown {
			aliveList = append(aliveList, node)
		}
		if rf.me == node {
			if flagNodeIsDown {
				DPrintfNew(ErrorLevel, "I'm node %v and my pref list says I'm down", rf.me)
			}
		}
	}
	sort.Ints(aliveList)
	return aliveList
}

func (rf *Dynamo) failureDetection() {
	for {
		time.Sleep(time.Duration(rf.failureDetectionPeriodms) * time.Millisecond)

		if rf.killed() {
			break
		}

		// rf.mu.Lock()

		args := DynamoPingArgs{}
		reply := DynamoPingReply{}

		// get a list of nodes which are alive
		aliveList := rf.getListLiveNodes()
		if len(aliveList) == 1 {
			// I'm the only alive node, so do nothing...
		} else {
			// ping a random node, but not ourself
			var peerNumber int
			for {
				peerNumber = rand.Intn(len(aliveList))
				peerNumber = aliveList[peerNumber]
				if peerNumber != rf.me {
					break
				}
			}
			ack := rf.peers[peerNumber].Call("Dynamo.DynamoPing", &args, &reply, rf.RPCtimeout)
			if !ack {
				// update local preference list
				for i := 1; i < rf.nodeCount; i++ {
					intendedNode := (peerNumber + i) % rf.nodeCount
					if len(rf.prefList[intendedNode]) != 0 {
						// update the node with the tmp values
						newValue := []int{}
						newValue = append(newValue, rf.prefList[intendedNode]...)
						newValue = append(newValue, rf.prefList[peerNumber]...)
						rf.prefList[intendedNode] = newValue
						rf.prefList[peerNumber] = []int{}
						break
					}
				}

				// send out new preference list
				secondArgs := DynamoUpdatePrefArgs{}
				secondReply := DynamoUpdatePrefReply{}
				secondArgs.UpdatedPrefList = make(map[int][]int)
				for node, value := range rf.prefList {
					tmp := make([]int, len(value))
					copy(tmp, value)
					secondArgs.UpdatedPrefList[node] = tmp
				}

				// loop through all nodes that this node thinks is alive and send below RPC to them
				for i := 0; i < rf.nodeCount; i++ {
					if len(rf.prefList[i]) != 0 && (i != rf.me) {
						secondAck := rf.peers[i].Call("Dynamo.DynamoUpdatePrefList", &secondArgs, &secondReply, rf.RPCtimeout)
						if !secondAck {
							DPrintfNew(InfoLevel, "Node %v unable to receive UpdatePrefList ack from node: %v", rf.me, i)
						}
					}
				}

				DPrintfNew(DebugLevel, "Node %v found a failed node: %v", rf.me, peerNumber)
			}
			// else {
			// 	DPrintfNew(DebugLevel, "Node %v successfully ping'd node: %v", rf.me, peerNumber)
			// }
		}
		// rf.mu.Unlock()
	}
}

func (rf *Dynamo) doSomething() {
	for {
		time.Sleep(1000 * time.Millisecond)
		if rf.killed() {
			break
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
func Make(peers []*labrpc.ClientEnd, me int, replicaCount int, quorumR int, quorumW int) *Dynamo {
	rf := &Dynamo{}

	rf.peers = peers
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.nodeCount = len(rf.peers) - 1
	rf.replicas = replicaCount
	rf.quorumR = quorumR
	rf.quorumW = quorumW

	rf.keyValue = make(map[string]ValueField)
	rf.prefList = make(map[int][]int)
	rf.ShiVizVClock = vclock.New()
	for i := 0; i < rf.nodeCount; i++ {
		rf.prefList[i] = []int{i}
	}

	// RPCtimeout must be strictly less than failure detection timeout!
	rf.failureDetectionPeriodms = 250
	// rf.failureDetectionPeriodms = rand.Intn(rf.nodeCount*rf.replicas)*2 + 100
	rf.RPCtimeout = 100

	if rf.me == rf.nodeCount {
		// for client do nothing
	} else {
		// launch failure detector for regular dynamo nodes
		go rf.failureDetection()
	}

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

	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())
	// if len(rf.log) > 0 {
	// 	rf.lastApplied = len(rf.log) - 1
	// }

	return rf
}
