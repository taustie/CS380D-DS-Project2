package dynamo

//
// Dynamo tests.
//

import (
	"fmt"
	"math/rand"
	"testing"

	// "github.com/jbondeson/vclock"
	"github.com/DistributedClocks/GoVector/govec/vclock"
)

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
// const RaftElectionTimeout = 1000 * time.Millisecond

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
	cfg.begin("Test (2A): use minimum quorum size during normal operation")

	// Set the client to cluster and vice versa timeout parameter
	// To do: In dynamo.go need a dynamo node timeout parameters for its failure detector

	test_count := 100
	for i := 0; i < test_count; i++ {
		keyString := fmt.Sprintf("\"Key number: %d\"", i)
		object := i
		var context Context
		args := PutArgs{keyString, object, context}
		reply := PutReply{}
		peerNumber := rand.Intn(nodes)
		rfClient := cfg.dynamoNodes[nodes]
		ack := rfClient.peers[peerNumber].Call("Dynamo.Put", &args, &reply)
		if !ack {
			t.Fatalf("No ack received from dynamo cluster on put(%s, nil, %d)", keyString, args.Object)
		}
	}

	for i := 0; i < test_count; i++ {
		keyString := fmt.Sprintf("\"Key number: %d\"", i)
		args := GetArgs{keyString}
		reply := GetReply{}
		peerNumber := rand.Intn(nodes)
		rfClient := cfg.dynamoNodes[nodes]
		DPrintfNew(InfoLevel, "Called Get()")
		ack := rfClient.peers[peerNumber].Call("Dynamo.Get", &args, &reply)
		if ack == false {
			t.Fatalf("Failed to receive ack from dynamo cluster on get(%s)", keyString)
		}
		if len(reply.Object) != 1 {
			t.Fatalf("Received too many or too few return values: %d on get(%s)", len(reply.Object), keyString)
		}
		DPrintfNew(InfoLevel, "Dynamo reply.Object[0]: %v", reply.Object[0])
		if reply.Object[0] != i {
			t.Fatalf("get(%s) returned value %d which does not match expected value %d", keyString, reply.Object[0], i)
		}
	}

	cfg.end()
}

// Description:
// 1) Configure: # nodes > 5, # replicas >= 5, R = all replicas, W = all replicas
// 2) Send many put requests to the Dynamo cluster using normal operating conditions (no node failures)
// 3) Send many get requests to the Dynamo cluster.
// 4) Check if all values match expected value and if any fail since a quorum must handle response.
func TestNormalMaxQuorum2A(t *testing.T) {
	// *** functions implemented in dynamo_wrapper and called here ***
	nodes := 100
	replicas := 90
	quorumR := replicas
	quorumW := replicas
	// Start-up dynamo (Config # nodes, # replicas, R, W, bring all nodes online, generate the static preference list, enable the network)
	cfg := make_config(t, nodes, replicas, quorumR, quorumW, false)
	defer cfg.cleanup()
	cfg.begin("Test (2A): use maximum quorum size during normal operation")

	// Set the client to cluster and vice versa timeout parameter
	// To do: In dynamo.go need a dynamo node timeout parameters for its failure detector

	test_count := 1000
	for i := 0; i < test_count; i++ {
		keyString := fmt.Sprintf("\"Key number: %d\"", i)
		object := i
		var context Context
		args := PutArgs{keyString, object, context}
		reply := PutReply{}
		peerNumber := rand.Intn(nodes)
		rfClient := cfg.dynamoNodes[nodes]
		ack := rfClient.peers[peerNumber].Call("Dynamo.Put", &args, &reply)
		if !ack {
			t.Fatalf("No ack received from dynamo cluster on put(%s, nil, %d)", keyString, args.Object)
		}
	}

	for i := 0; i < test_count; i++ {
		keyString := fmt.Sprintf("\"Key number: %d\"", i)
		args := GetArgs{keyString}
		reply := GetReply{}
		peerNumber := rand.Intn(nodes)
		rfClient := cfg.dynamoNodes[nodes]
		DPrintfNew(InfoLevel, "Called Get()")
		ack := rfClient.peers[peerNumber].Call("Dynamo.Get", &args, &reply)
		if ack == false {
			t.Fatalf("Failed to receive ack from dynamo cluster on get(%s)", keyString)
		}
		if len(reply.Object) != 1 {
			t.Fatalf("Received too many or too few return values: %d on get(%s) in value: %v", len(reply.Object), keyString, reply.Object)
		}
		DPrintfNew(InfoLevel, "Dynamo reply.Object[0]: %v", reply.Object[0])
		if reply.Object[0] != i {
			t.Fatalf("get(%s) returned value %d which does not match expected value %d", keyString, reply.Object[0], i)
		}
	}

	cfg.end()
}

func TestVectorClockEx(t *testing.T) {
	n1 := vclock.New()
	n2 := vclock.New()

	n1.Set("a", 1)
	n1.Set("b", 2)
	n1.Set("d", 3)
	n2.Set("a", 1)
	n2.Set("b", 1)
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

	fmt.Println(n1)
	fmt.Println(n2)
}

func failComparison(t *testing.T, failMessage string, clock1, clock2 vclock.VClock) {
	t.Fatalf(failMessage, clock1.ReturnVCString(), clock2.ReturnVCString())
}

// Description:
// 1) Configure: # nodes = arbitrary, # replicas = arbitrary, R = # replicas, W = # replicas
// 2) Write multiple values to dynamo.
// 3) Eventually, trigger a coordinator node failure.
// 4) Verify the failure detector detects the node has failed after some time,
// 5) Wait for updates to the global preference list.
// 6) Write multiple updates to dynamo and verify all are successful (hinted handoff worked)
func TestSingleNodeFailure2B(t *testing.T) {

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
