package dynamo

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
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

	keyValue                 map[string][]ValueField
	prefList                 map[int][]int
	failureDetectionPeriodms int
	RPCtimeout               int
	ShiVizVClock             vclock.VClock
}

// *************************** Helper Functions/Methods ***************************

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

func contains(value []int, token int) bool {
	for _, v := range value {
		if v == token {
			return true
		}
	}
	return false
}

func (rf *Dynamo) findDefaultCoordinator(key string) int {
	// perform the hash to determine the expected coordinator node
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
	return actualCoordNode
}

func (rf *Dynamo) findCoordinator(key string) int {
	// perform the hash to determine the expected coordinator node
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

	// find out who actually owns this node's keys in the preference list
	aliveList := rf.getListLiveNodes()
	for i := 0; i < len(aliveList); i++ {
		result := contains(rf.prefList[aliveList[i]], expectedCoordNodeInt)
		if result {
			actualCoordNode = aliveList[i]
			break
		}
	}
	return actualCoordNode
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

// type PutArgs struct {
// 	Key     string //[]byte
// 	Object  int
// 	Context Context
// }

func (rf *Dynamo) coordinatorApplyPut(args *PutArgs, reply *PutReply) {
	if len(rf.keyValue[args.Key]) == 1 {
		// one data entry already present
		rf.keyValue[args.Key][0].Data = args.Object
		meString := strconv.Itoa(rf.me)
		rf.keyValue[args.Key][0].Timestamp.Tick(meString)
		DPrintfNew(NoticeLevel, "Coord existing value already present on put.  New vector clock value is: %v", rf.keyValue[args.Key])
	} else if len(rf.keyValue[args.Key]) == 0 {
		// no entry present
		value := ValueField{}
		value.Data = args.Object
		value.Timestamp = vclock.New()
		meString := strconv.Itoa(rf.me)
		value.Timestamp.Set(meString, 1)
		rf.keyValue[args.Key] = append(rf.keyValue[args.Key], value)
	} else {
		// More than one entry already present
		// multiple concurrent values to comb through for the correct context
		DPrintfNew(ErrorLevel, "Multiple concurrent values, and no logic to choose the one to update")
		rf.keyValue[args.Key][0].Data = args.Object
		meString := strconv.Itoa(rf.me)
		rf.keyValue[args.Key][0].Timestamp.Tick(meString)
	}
}

// apply the put request to the coordinator node
func (rf *Dynamo) dynamoPut(args *PutArgs, reply *PutReply) {
	rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
	// Add to log
	DPrintfNew(ShiVizLevel, "event: DynamoPutObjectSelf\thost: %v\tclock:%v", rf.me, rf.ShiVizVClock.ReturnVCString())
	// apply the request to the current dynamo node
	rf.coordinatorApplyPut(args, reply)

	var value ValueField
	if len(rf.keyValue[args.Key]) == 1 {
		value = rf.keyValue[args.Key][0]
	} else {
		DPrintfNew(ErrorLevel, "Multiple concurrent values, and no logic to choose the one to update")
		value = rf.keyValue[args.Key][0]
	}

	writeCount := 1

	// put the data on a write quorum of replicas
	if writeCount < rf.quorumW {
		// Waiting for a subset of go routines to complete
		// Use https://stackoverflow.com/questions/52227954/waitgroup-on-subset-of-go-routines

		// buffered channel up to read quorum - 1 returns
		ackChan := make(chan int, rf.quorumW-1)
		dynamoReply := make([]DynamoPutReply, rf.replicas-1)

		// find which replica to send data to
		for i := 0; i < rf.replicas-1; i++ {
			updatedArgs := DynamoPutArgs{}
			updatedArgs.Key = args.Key
			updatedArgs.Object = value
			// Replicas are chose based on real-time preference list
			aliveList := rf.getListLiveNodes()
			var myIndexAliveList int
			if len(aliveList) < rf.quorumW {
				DPrintfNew(ErrorLevel, "A write quorum is not available for put request!")
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
			replicaNode := aliveList[intendedNodeIndex]
			go rf.sendReplicaData(ackChan, i, replicaNode, &updatedArgs, &dynamoReply[i])
		}
		for responseCount := 0; responseCount < rf.quorumW-1; responseCount++ {
			index := <-ackChan
			reply.VClock = rf.ShiVizVClock
			DPrintfNew(InfoLevel, "Coordinator: %v received ack from replica: %v on put request", rf.me, dynamoReply[index].NodeID)
		}
	}
}

//ReturnVCString returns a string encoding of a vector clock
func ReturnVCStringSorted(vc *vclock.VClock) string {
	//sort
	ids := make([]string, len(*vc))
	i := 0
	for id := range *vc {
		ids[i] = id
		i++
	}

	sort.Strings(ids)

	var buffer bytes.Buffer
	buffer.WriteString("{")
	for i := range ids {
		buffer.WriteString(fmt.Sprintf("\"%s\":%d", ids[i], (*vc)[ids[i]]))
		if i+1 < len(ids) {
			buffer.WriteString(", ")
		}
	}
	buffer.WriteString("}")
	return buffer.String()
}

func (rf *Dynamo) dynamoGet(args *GetArgs, reply *GetReply) {
	readCount := 1
	ObjectFrequency := make(map[string]int)
	ObjectPtr := make(map[string]*ValueField)
	for index, value := range rf.keyValue[args.Key] {
		vcString := ReturnVCStringSorted(&value.Timestamp)
		ObjectFrequency[vcString]++
		ObjectPtr[vcString] = &(rf.keyValue[args.Key][index])
	}

	tempObject := []int{}
	tempVectorTimestamp := vclock.New()
	dynamoReply := make([]DynamoGetReply, rf.replicas-1)

	if readCount < rf.quorumR {
		replicaGetArgs := GetArgs{args.Key, args.VClock.Copy()}
		// Waiting for a subset of go routines to complete
		// Use https://stackoverflow.com/questions/52227954/waitgroup-on-subset-of-go-routines

		// buffered channel up to read quorum - 1 returns
		ackChan := make(chan int, rf.quorumR-1)
		for i := 0; i < rf.replicas-1; i++ {
			// Replicas are chose based on real-time preference list
			aliveList := rf.getListLiveNodes()
			var myIndexAliveList int
			if len(aliveList) < rf.quorumR {
				DPrintfNew(ErrorLevel, "A read quorum is not available for get request!")
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
			replicaNode := aliveList[intendedNodeIndex]
			go rf.requestReplicaData(ackChan, i, replicaNode, &replicaGetArgs, &dynamoReply[i])
		}
		for responseCount := 0; responseCount < rf.quorumR-1; responseCount++ {
			index := <-ackChan
			reply.VClock = rf.ShiVizVClock
			DPrintfNew(InfoLevel, "Coordinator: %v received ack from replica: %v on get request with value: %v", rf.me, dynamoReply[index].NodeID, dynamoReply[index])
			for objectIndex, value := range dynamoReply[index].Object {
				vcString := ReturnVCStringSorted(&value.Timestamp)
				ObjectFrequency[vcString]++
				ObjectPtr[vcString] = &(dynamoReply[index].Object[objectIndex])
			}
		}
	}

	// do conflict resolution
	for key, value := range ObjectFrequency {
		if value >= rf.quorumR {
			tempObject = append(tempObject, ObjectPtr[key].Data)
			tempVectorTimestamp.Merge(ObjectPtr[key].Timestamp)
		}
	}

	reply.Object = tempObject
	reply.Context.VectorTimestamp = tempVectorTimestamp.Copy()
	reply.Context.IntendedNode = rf.findDefaultCoordinator(args.Key)

	DPrintfNew(NoticeLevel, "Get about to reply with: %v", reply)
	return
}

// type GetReply struct {
// 	Object  []int
// 	Context Context
// }

// type DynamoGetReply struct {
// 	NodeID int
// 	Object []ValueField
// }

// *************************** Dynamo API ***************************

func (rf *Dynamo) RouteGetToCoordinator(args *GetArgs, reply *GetReply) {
	// Merge and Tick
	rf.ShiVizVClock.Merge(args.VClock)
	rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
	// Add to log
	DPrintfNew(ShiVizLevel, "event: RouteGetToCoordinatorHandler\thost: %v\tclock:%v", rf.me, rf.ShiVizVClock.ReturnVCString())
	DPrintfNew(InfoLevel, "Coordinator: %v is processing get request", rf.me)
	// verify this node is the coorect coordinator
	coordinatorNode := rf.findCoordinator(args.Key)
	if rf.me != coordinatorNode {
		DPrintfNew(ErrorLevel, "Put request routed to incorrect coordinator node: %v", coordinatorNode)
		return
	}

	rf.dynamoGet(args, reply)
	// should this be included?
	//reply.VClock = rf.ShiVizVClock
	return
}

func (rf *Dynamo) RoutePutToCoordinator(args *PutArgs, reply *PutReply) {
	// Merge and Tick
	rf.ShiVizVClock.Merge(args.VClock)
	rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
	// Add to log
	DPrintfNew(ShiVizLevel, "event: RoutePutToCoordinatorHandler\thost: %v\tclock:%v", rf.me, rf.ShiVizVClock.ReturnVCString())
	DPrintfNew(InfoLevel, "Coordinator: %v is processing put request", rf.me)
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
	Object []ValueField
	VClock vclock.VClock
}

func (rf *Dynamo) DynamoGetObject(args *GetArgs, dynamoReply *DynamoGetReply) {
	// Merge and Tick
	rf.ShiVizVClock.Merge(args.VClock)
	rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
	// Add to log
	DPrintfNew(ShiVizLevel, "event: GetObjectHandler\thost: %v\tclock:%v", rf.me, rf.ShiVizVClock.ReturnVCString())
	dynamoReply.NodeID = rf.me
	dynamoReply.Object = make([]ValueField, len(rf.keyValue[args.Key]))
	copy(dynamoReply.Object, rf.keyValue[args.Key])
	dynamoReply.VClock = rf.ShiVizVClock.Copy()
}

func (rf *Dynamo) requestReplicaData(ackChan chan int, index int, replicaNode int, args *GetArgs, dynamoReply *DynamoGetReply) {
	rf.mu.Lock()
	// Tick Node
	rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
	// Add to args
	args.VClock = rf.ShiVizVClock.Copy()
	// Add to log
	DPrintfNew(ShiVizLevel, "event: GetObject\thost: %v\tclock:%v", rf.me, args.VClock.ReturnVCString())
	rf.mu.Unlock()
	DPrintfNew(InfoLevel, "Sending get request to replica: %v ", replicaNode)
	ack := rf.peers[replicaNode].Call("Dynamo.DynamoGetObject", args, dynamoReply, -1) // Not sure
	if ack == true {
		rf.mu.Lock()
		// Merge and Tick
		rf.ShiVizVClock.Merge(dynamoReply.VClock)
		rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
		// Add to log
		DPrintfNew(ShiVizLevel, "event: DynamoGetObjectAck\thost: %v\tclock:%v", rf.me, rf.ShiVizVClock.ReturnVCString())
		// return index that responded
		ackChan <- index
		rf.mu.Unlock()
	} else {
		DPrintfNew(WarningLevel, "Replica: %v timed out on get request", replicaNode)
	}
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

func (rf *Dynamo) ReplicaPutObject(dynamoArgs *DynamoPutArgs, dynamoReply *DynamoPutReply) {
	// Merge and Tick
	rf.ShiVizVClock.Merge(dynamoArgs.VClock)
	rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
	// Add to log
	DPrintfNew(ShiVizLevel, "event: ReplicaPutObjectHandler\thost: %v\tclock:%v", rf.me, rf.ShiVizVClock.ReturnVCString())
	dynamoReply.NodeID = rf.me

	if len(rf.keyValue[dynamoArgs.Key]) == 1 {
		// one data entry already present
		rf.keyValue[dynamoArgs.Key][0].Data = dynamoArgs.Object.Data
		rf.keyValue[dynamoArgs.Key][0].Timestamp = dynamoArgs.Object.Timestamp.Copy()
		DPrintfNew(NoticeLevel, "Replica existing value already present on put.  New vector clock value is: %v", rf.keyValue[dynamoArgs.Key])
	} else if len(rf.keyValue[dynamoArgs.Key]) == 0 {
		// no entry present
		newField := ValueField{}
		newField.Data = dynamoArgs.Object.Data
		newField.Timestamp = dynamoArgs.Object.Timestamp.Copy()
		rf.keyValue[dynamoArgs.Key] = append(rf.keyValue[dynamoArgs.Key], newField)
	} else {
		// More than one entry already present
		// multiple concurrent values to comb through for the correct context
		DPrintfNew(ErrorLevel, "Multiple concurrent values, and no logic to choose the one to update")
		rf.keyValue[dynamoArgs.Key][0].Data = dynamoArgs.Object.Data
		rf.keyValue[dynamoArgs.Key][0].Timestamp = dynamoArgs.Object.Timestamp.Copy()
	}
	dynamoReply.VClock = rf.ShiVizVClock.Copy()
}

func (rf *Dynamo) sendReplicaData(ackChan chan int, index int, replicaNode int, args *DynamoPutArgs, dynamoReply *DynamoPutReply) {
	rf.mu.Lock()
	// Tick Node
	rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
	// Add to args
	args.VClock = rf.ShiVizVClock.Copy()
	// Add to log
	DPrintfNew(ShiVizLevel, "event: DynamoPutObject\thost: %v\tclock:%v", rf.me, args.VClock.ReturnVCString())
	rf.mu.Unlock()
	DPrintfNew(InfoLevel, "Sending put request to replica: %v ", replicaNode)
	ack := rf.peers[replicaNode].Call("Dynamo.ReplicaPutObject", args, dynamoReply, -1) // Not sure
	if ack == true {
		rf.mu.Lock()
		// Merge and Tick
		rf.ShiVizVClock.Merge(dynamoReply.VClock)
		rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
		// Add to log
		DPrintfNew(ShiVizLevel, "event: DynamoPutObjectAck\thost: %v\tclock:%v", rf.me, rf.ShiVizVClock.ReturnVCString())
		// respond to RPC by returning my index
		ackChan <- index
		rf.mu.Unlock()
	} else {
		DPrintfNew(WarningLevel, "Replica: %v timed out on put request", replicaNode)
	}
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
		rf.prefList[node] = tmp
	}
	DPrintfNew(InfoLevel, "Node %v prefList (after): %v", rf.me, rf.prefList)
	return
}

// *************************** Periodic Go Routines ***************************

func (rf *Dynamo) failureDetection() {
	for {
		time.Sleep(time.Duration(rf.failureDetectionPeriodms) * time.Millisecond)

		if rf.killed() {
			break
		}

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

	rf.keyValue = make(map[string][]ValueField)
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
