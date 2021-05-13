package dynamo

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
		DPrintfNew(InfoLevel, "Get operation already at correct coordinator %v", coordinatorNode)
		rf.dynamoGet(args, reply)
	} else {
		DPrintfNew(InfoLevel, "Get operation being routed to coordinator %v", coordinatorNode)
		rf.peers[coordinatorNode].Call("Dynamo.RouteGetToCoordinator", args, reply, -1)
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
	coordinatorNode := rf.findCoordinator(args.Key)
	if rf.me == coordinatorNode {
		// Current dynamo node is also the correct coordinator for this request
		//go rf.dynamoPut(args, reply)
		DPrintfNew(InfoLevel, "Put operation with value: %v already at correct coordinator %v", coordinatorNode, args.Object)
		rf.dynamoPut(args, reply)
	} else {
		DPrintfNew(InfoLevel, "Put operation with value: %v being routed to coordinator %v", coordinatorNode, args.Object)
		rf.peers[coordinatorNode].Call("Dynamo.RoutePutToCoordinator", args, reply, -1)
	}
	return
}
