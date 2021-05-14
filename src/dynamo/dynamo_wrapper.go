package dynamo

import (
	"strconv"

	"github.com/DistributedClocks/GoVector/govec/vclock"
)

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
	// Merge and Tick
	rf.ShiVizVClock.Merge(args.VClock)
	rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
	// Add to log
	DPrintfNew(ShiVizLevel, "event: GetHandler\thost: %v\tclock:%v", rf.me, rf.ShiVizVClock.ReturnVCString())
	coordinatorNode := rf.findCoordinator(args.Key)
	if rf.me == coordinatorNode {
		// Current dynamo node is also the correct coordinator for this request
		// cannot put in goroutine since reply is not available synchronously!
		DPrintfNew(InfoLevel, "Get operation already at correct coordinator %v", coordinatorNode)
		rf.dynamoGet(args, reply)
	} else {
		// Tick Node
		rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
		// Add to args
		args.VClock = rf.ShiVizVClock
		// Add to log
		DPrintfNew(ShiVizLevel, "event: RouteGetToCoordinator\thost: %v\tclock:%v", rf.me, args.VClock.ReturnVCString())
		DPrintfNew(InfoLevel, "Get operation being routed to coordinator %v", coordinatorNode)
		ack := rf.peers[coordinatorNode].Call("Dynamo.RouteGetToCoordinator", args, reply, -1)

		if ack {
			// Merge and Tick
			rf.ShiVizVClock.Merge(reply.VClock)
			rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
			// Add to log
			DPrintfNew(ShiVizLevel, "event: RouteGetToCoordinatorAck\thost: %v\tclock:%v", rf.me, rf.ShiVizVClock.ReturnVCString())
		}
	}
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
	// Merge and Tick
	rf.ShiVizVClock.Merge(args.VClock)
	rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
	// Add to log
	DPrintfNew(ShiVizLevel, "event: PutHandler\thost: %v\tclock:%v", rf.me, rf.ShiVizVClock.ReturnVCString())

	coordinatorNode := rf.findCoordinator(args.Key)
	if rf.me == coordinatorNode {
		// Current dynamo node is also the correct coordinator for this request
		//go rf.dynamoPut(args, reply)
		DPrintfNew(InfoLevel, "Put operation with value: %v already at correct coordinator %v", coordinatorNode, args.Object)
		rf.dynamoPut(args, reply)
	} else {
		// Tick Node
		rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
		// Add to args
		args.VClock = rf.ShiVizVClock
		// Add to log
		DPrintfNew(ShiVizLevel, "event: RoutePutToCoordinator\thost: %v\tclock:%v", rf.me, args.VClock.ReturnVCString())
		DPrintfNew(InfoLevel, "Put operation with value: %v being routed to coordinator %v", coordinatorNode, args.Object)
		ack := rf.peers[coordinatorNode].Call("Dynamo.RoutePutToCoordinator", args, reply, -1)

		if ack {
			// Merge and Tick
			rf.ShiVizVClock.Merge(reply.VClock)
			rf.ShiVizVClock.Tick(strconv.Itoa(rf.me))
			// Add to log
			DPrintfNew(ShiVizLevel, "event: RoutePutToCoordinatorAck\thost: %v\tclock:%v", rf.me, rf.ShiVizVClock.ReturnVCString())
		}
	}
	reply.VClock = rf.ShiVizVClock
	return
}
