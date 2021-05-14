package dynamo

//
// Dynamo tests.
//

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/DistributedClocks/GoVector/govec/vclock"
)

// Description:
// 1) Configure: # nodes = arbitrary, # replicas = arbitrary, R = 1, W = 1
// 2) Send many put requests to the Dynamo cluster using normal operating conditions (no node failures)
// 3) Send many get requests to the Dynamo cluster.
// 4) Verify all values match expected value since coordinator will handle response.
func TestNormalMinQuorum2A(t *testing.T) {
	// *** functions implemented in dynamo_wrapper and called here ***
	nodes := 10
	replicas := 3
	quorumR := 1
	quorumW := 1
	// Start-up dynamo (Config # nodes, # replicas, R, W, bring all nodes online, generate the static preference list, enable the network)
	cfg := make_config(t, nodes, replicas, quorumR, quorumW, false)
	defer cfg.cleanup()
	cfg.begin("Test (2A): normal operation with minimum quorum size")

	// Note:
	// Client to cluster timeout is infinite usually. When node is disconnected it uses a
	// random timeout.
	// See Make() for dynamo node RPC timeout parameters
	// See Make() for dynamo failure detector timeout (must be greater than RPC timeout)
	test_count := 100
	avoidNodeList := []int{}
	cfg.putData("Key number: ", test_count, nodes, avoidNodeList, -70, -40)
	cfg.getData("Key number: ", test_count, nodes, avoidNodeList, -70, -40)
	cfg.end()
}

// Description:
// 1) Configure: # nodes > 5, # replicas >= 5, R = all replicas, W = all replicas
// 2) Send many put requests to the Dynamo cluster using normal operating conditions (no node failures)
// 3) Send many get requests to the Dynamo cluster.
// 4) Check if all values match expected value and if any fail since a quorum must handle response.
func TestNormalMaxQuorum2A(t *testing.T) {
	// *** functions implemented in dynamo_wrapper and called here ***
	nodes := 10
	replicas := 3
	quorumR := replicas
	quorumW := replicas
	// Start-up dynamo (Config # nodes, # replicas, R, W, bring all nodes online, generate the static preference list, enable the network)
	cfg := make_config(t, nodes, replicas, quorumR, quorumW, false)
	defer cfg.cleanup()
	cfg.begin("Test (2A): normal operation with maximum quorum size")
	test_count := 100
	avoidNodeList := []int{}
	cfg.putData("Key number: ", test_count, nodes, avoidNodeList, 0, 1)
	cfg.getData("Key number: ", test_count, nodes, avoidNodeList, 0, 1)
	cfg.end()
}

// Description:
// 1) Configure: # nodes > 5, # replicas >= 5, R = all replicas, W = all replicas
// 2) Only failure detect messages exchanged, and verify all nodes remain online
func TestNoActivity2A(t *testing.T) {
	// *** functions implemented in dynamo_wrapper and called here ***
	nodes := 10
	replicas := 3
	quorumR := replicas
	quorumW := replicas
	// Start-up dynamo (Config # nodes, # replicas, R, W, bring all nodes online, generate the static preference list, enable the network)
	cfg := make_config(t, nodes, replicas, quorumR, quorumW, false)
	defer cfg.cleanup()
	cfg.begin("Test (2A): dynamo nodes run failure detector without any failures")
	// check continuously for 10 seconds
	for i := 0; i < 100; i++ {
		peerNumber := rand.Intn(nodes)
		cfg.checkAllAlive(peerNumber)
		time.Sleep(100 * time.Millisecond)
	}
	cfg.end()
}

// Description:
// 1) Configure: # nodes = arbitrary, # replicas = arbitrary, R = # replicas, W = # replicas
// 2) Write multiple values to dynamo.
// 3) Eventually, trigger a coordinator node failure.
// 4) Verify the failure detector detects the node has failed after some time,
// 5) Wait for updates to the global preference list.
// 6) Write multiple updates to dynamo and verify all are successful (hinted handoff worked)
func TestSingleNodeFailure2B(t *testing.T) {
	// *** functions implemented in dynamo_wrapper and called here ***
	nodes := 10
	replicas := 3
	quorumR := replicas
	quorumW := replicas
	// Start-up dynamo (Config # nodes, # replicas, R, W, bring all nodes online, generate the static preference list, enable the network)
	cfg := make_config(t, nodes, replicas, quorumR, quorumW, false)
	defer cfg.cleanup()
	cfg.begin("Test (2B): a single node fails")

	test_count := 100
	avoidNodeList := []int{}
	cfg.putData("Key number: ", test_count, nodes, avoidNodeList, 0, 1)
	removedNode := rand.Intn(nodes)
	cfg.disconnect(removedNode)
	avoidNodeList = append(avoidNodeList, removedNode)
	fmt.Println("Node ", removedNode, "is down")
	var inspectNode int
	if removedNode == 0 {
		inspectNode = 1
	} else {
		inspectNode = 0
	}
	cfg.checkForFailure(inspectNode, removedNode, 3)
	cfg.putData("Key: ", test_count, nodes, avoidNodeList, 2, 1)
	cfg.getData("Key: ", test_count, nodes, avoidNodeList, 2, 1)
	DPrintfNew(InfoLevel, "removedNode prefList is: %v", cfg.dynamoNodes[removedNode].prefList)
	cfg.end()
}

// Sidenote: Cannot have concurrent context info on put from client
// compared with context on coordinator becuase:
// 1) Network Partition - client cannot simultaneously talk to both partitions.
// as long as partition is present client must also be isolated.
// 2) Hinted Handoff case - we don't restore the revived node in global pref list
// until we've achieved consistency.

// Description:
// 1) Configure: # nodes = arbitrary, # replicas = arbitrary, R = # replicas, W = # replicas
// 2) Write multiple values to dynamo.
// 3) Eventually, trigger a coordinator node failure.
// 4) Verify the failure detector detects the node has failed after some time,
// 5) Wait for updates to the global preference list.
// 6) Write multiple updates to dynamo and verify all are successful (hinted handoff worked)
func TestNormalUpdatesMaxQuorum2B(t *testing.T) {
	// *** functions implemented in dynamo_wrapper and called here ***
	nodes := 10
	replicas := 3
	quorumR := replicas
	quorumW := replicas
	// Start-up dynamo (Config # nodes, # replicas, R, W, bring all nodes online, generate the static preference list, enable the network)
	cfg := make_config(t, nodes, replicas, quorumR, quorumW, false)
	defer cfg.cleanup()
	cfg.begin("Test (2B): perform normal update operation with maximum quorum size")
	test_count := 100
	avoidNodeList := []int{}

	// ********************* round 1: initial put operations *********************
	keyStart := 0
	valueStart := 1
	inputKey := "Key number: "
	cfg.putData(inputKey, test_count, nodes, avoidNodeList, keyStart, valueStart)

	// ********************* round 2: get operations *********************
	// retrieve the context information
	returnedData := make([][]int, test_count)
	returnedContext := make([]Context, test_count)
	for i := 0; i < test_count; i++ {
		expectedKey := keyStart + i
		expectedValue := valueStart + i
		keyString := fmt.Sprintf("%s%d", inputKey, expectedKey)
		// Tick Node
		rfClient := cfg.dynamoNodes[nodes]
		rfClient.ShiVizVClock.Tick("Client")
		args := GetArgs{keyString, rfClient.ShiVizVClock}
		reply := GetReply{}
		var peerNumber int
		for {
			peerNumber = rand.Intn(nodes)
			result := contains(avoidNodeList, peerNumber)
			if result == false {
				break
			}
		}
		DPrintfNew(ShiVizLevel, "event: Get\thost: %v\tclock:%v", "Client", args.VClock.ReturnVCString())
		ack := rfClient.peers[peerNumber].Call("Dynamo.Get", &args, &reply, -1)
		if ack == false {
			cfg.t.Fatalf("Failed to receive ack from dynamo cluster on get(%s)", keyString)
		}
		// Merge and Tick
		rfClient.ShiVizVClock.Merge(reply.VClock)
		rfClient.ShiVizVClock.Tick("Client")
		DPrintfNew(ShiVizLevel, "event: GetAck\thost: %v\tclock:%v", "Client", rfClient.ShiVizVClock.ReturnVCString())
		if len(reply.Object) != 1 {
			cfg.t.Fatalf("Received too many or too few return values: %d on get(%s)", len(reply.Object), keyString)
		}
		if reply.Object[0] != expectedValue {
			cfg.t.Fatalf("Received a value: %d from node: %d, which does not match expected: %v", reply.Object, peerNumber, expectedValue)
		}
		returnedData[i] = make([]int, len(reply.Object))
		copy(returnedData[i], reply.Object)
		returnedContext[i] = reply.Context
		oldestClock := returnedContext[i].VectorTimestamp.LastUpdate()
		DPrintfNew(NoticeLevel, "Returned context was %v, and expected: %v", returnedContext[i].VectorTimestamp, 1)
		if oldestClock != 1 {
			cfg.t.Fatalf("Returned context was %v, which does not match expected: %v", returnedContext[i].VectorTimestamp, 1)
		}
	}

	// ********************* round 3: more put operations that overwrite round 1 *********************
	valueStart = -100
	for i := 0; i < test_count; i++ {
		expectedKey := keyStart + i
		expectedValue := valueStart + i
		keyString := fmt.Sprintf("%s%d", inputKey, expectedKey)
		object := expectedValue
		// can keep as blank and should be descendent
		var context Context
		// alternatively use latest context
		// context := returnedContext[i]
		rfClient := cfg.dynamoNodes[nodes]
		rfClient.ShiVizVClock.Tick("Client")
		args := PutArgs{keyString, object, context, rfClient.ShiVizVClock}
		reply := PutReply{}
		var peerNumber int
		for {
			peerNumber = rand.Intn(nodes)
			result := contains(avoidNodeList, peerNumber)
			if result == false {
				break
			}
		}
		DPrintfNew(ShiVizLevel, "event: Put\thost: %v\tclock:%v", "Client", args.VClock.ReturnVCString())
		ack := rfClient.peers[peerNumber].Call("Dynamo.Put", &args, &reply, -1)
		if !ack {
			cfg.t.Fatalf("Failed to receive ack from dynamo cluster on put(%s, nil, %d)", keyString, args.Object)
		}
		// Merge and Tick
		rfClient.ShiVizVClock.Merge(reply.VClock)
		rfClient.ShiVizVClock.Tick("Client")
		// Add to log
		DPrintfNew(ShiVizLevel, "event: PutAck\thost: %v\tclock:%v", "Client", rfClient.ShiVizVClock.ReturnVCString())
	}

	// ********************* round 4: verify round 3 updates applied and the context info is up to date *********************
	for i := 0; i < test_count; i++ {
		expectedKey := keyStart + i
		expectedValue := valueStart + i
		keyString := fmt.Sprintf("%s%d", inputKey, expectedKey)
		// Tick Node
		rfClient := cfg.dynamoNodes[nodes]
		rfClient.ShiVizVClock.Tick("Client")
		args := GetArgs{keyString, rfClient.ShiVizVClock}
		reply := GetReply{}
		var peerNumber int
		for {
			peerNumber = rand.Intn(nodes)
			result := contains(avoidNodeList, peerNumber)
			if result == false {
				break
			}
		}
		DPrintfNew(ShiVizLevel, "event: Get\thost: %v\tclock:%v", "Client", args.VClock.ReturnVCString())
		ack := rfClient.peers[peerNumber].Call("Dynamo.Get", &args, &reply, -1)
		if ack == false {
			cfg.t.Fatalf("Failed to receive ack from dynamo cluster on get(%s)", keyString)
		}
		// Merge and Tick
		rfClient.ShiVizVClock.Merge(reply.VClock)
		rfClient.ShiVizVClock.Tick("Client")
		DPrintfNew(ShiVizLevel, "event: GetAck\thost: %v\tclock:%v", "Client", rfClient.ShiVizVClock.ReturnVCString())
		if len(reply.Object) != 1 {
			cfg.t.Fatalf("Received too many or too few return values: %d on get(%s)", len(reply.Object), keyString)
		}
		if reply.Object[0] != expectedValue {
			cfg.t.Fatalf("Received a value: %d from node: %d, which does not match expected: %v", reply.Object, peerNumber, expectedValue)
		}
		oldestClock := reply.Context.VectorTimestamp.LastUpdate()
		DPrintfNew(NoticeLevel, "Returned context was %v, and expected: %v", reply.Context.VectorTimestamp, 2)
		if oldestClock != 2 {
			cfg.t.Fatalf("Returned context was %v, which does not match expected: %v", reply.Context.VectorTimestamp, 2)
		}
	}

	cfg.end()
}

// Description:
// 1) Configure: # nodes = arbitrary, # replicas = arbitrary, R = # replicas, W = # replicas
// 2) Perform TestSingleNodeFailure2B steps 2-6
// 4) Revive the coordinator node.
// 5) Wait for updates to the global preference list.
// 6) Once updated, target the revived coordinator using get requests.
// 7) Verify stale data is not returned
func TestStaleData2B(t *testing.T) {

}

// Description:
// 1) Configure: # nodes = arbitrary, # replicas = arbitrary, R = # replicas, W = # replicas
// 2)
func TestRingPartition2C(t *testing.T) {

}

// Description:
// 1) Configure: # nodes = arbitrary, # replicas = arbitrary, R = # replicas, W = # replicas
// 2)
func TestRingPartitionWithNodeFailure2C(t *testing.T) {

}

func TestVectorClockEx(t *testing.T) {
	n1 := vclock.New()
	n2 := vclock.New()

	n1.Set("a", 1)
	n1.Set("b", 2)
	n1.Set("c", 3)
	n2.Set("a", 1)
	n2.Set("b", 2)
	n2.Set("d", 3)

	if n1.Compare(n2, vclock.Equal) {
		failComparison(t, "Clocks are defined as Equal: n1 = %s | n2 = %s", n1, n2)
	} else if n1.Compare(n2, vclock.Ancestor) {
		failComparison(t, "Clocks are defined as Ancestor: n1 = %s | n2 = %s", n1, n2)
	} else if n1.Compare(n2, vclock.Descendant) {
		failComparison(t, "Clocks are defined as Descendant: n1 = %s | n2 = %s", n1, n2)
	} else if !n1.Compare(n2, vclock.Concurrent) {
		failComparison(t, "Clocks not defined as Concurrent: n1 = %s | n2 = %s", n1, n2)
	}

	if n2.Compare(n1, vclock.Equal) {
		failComparison(t, "Clocks are defined as Equal: n1 = %s | n2 = %s", n2, n1)
	} else if n2.Compare(n1, vclock.Ancestor) {
		failComparison(t, "Clocks are defined as Ancestor: n1 = %s | n2 = %s", n2, n1)
	} else if n2.Compare(n1, vclock.Descendant) {
		failComparison(t, "Clocks are defined as Descendant: n1 = %s | n2 = %s", n2, n1)
	} else if !n2.Compare(n1, vclock.Concurrent) {
		failComparison(t, "Clocks not defined as Concurrent: n1 = %s | n2 = %s", n2, n1)
	}

	// fmt.Println(n1)
	// fmt.Println(n2)
}

func failComparison(t *testing.T, failMessage string, clock1, clock2 vclock.VClock) {
	t.Fatalf(failMessage, clock1.ReturnVCString(), clock2.ReturnVCString())
}
