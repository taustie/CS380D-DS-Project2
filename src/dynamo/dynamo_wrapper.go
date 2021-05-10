package dynamo

// To do: implement get in dynamo_wrapper.go
// This function handles client get request, but we
// need intermediate layer for controlling the network.
// For requests that get through, these parameters are copied
// to a new message that is sent to dynamo cluster

// To do:
// *** functions implemented in dynamo_wrapper and called here ***
// Config # nodes, # replicas, R, W
// Start-up dynamo
// 		bring all nodes online - like the make function in raft
//		generate the static preference list - each node has an address (or nodeID, ex node 0), hash(key) mod (nodeID) = coordinator node
//		enable the network - look up table n x n grid for which nodes can communicate.  Also have client to node grid array
//		creating node outage - for any client or node request prevent data from reaching the node & always timeout
// Set the client to cluster timeout parameter
// To do: In dynamo.go need a dynamo node timeout parameters for failure detector

// hash(key) mod (nodeID) = pref list
//
// default:
// {nodeID, coordinator node}
// node 0 => 0
// node 1 => -1
// node 2 => 1,2
// ...
// node 9 => 9
