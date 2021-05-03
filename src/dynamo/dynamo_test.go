package dynamo

//
// Raft tests.
//
// we will use the original test_test.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"fmt"
	"testing"
	"time"
)

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const RaftElectionTimeout = 1000 * time.Millisecond

func put(key []byte, context []int, value int) bool {
	return true
}

func get(key []byte) (bool, []int, []int) {
	var value []int = []int{0}
	context := []int{0}
	return true, value, context
}

// Description: During normal operation with no node failures, test that all
// values are written to Dynamo clusters. We can expect all values to match despite
// being eventually consistent because the coordinator nodes will handle this test.
func TestNormalGetPut2A(t *testing.T) {
	// *** functions implemented in dynamo_wrapper and called here ***
	// Configure total number of nodes + total number of replicas
	// start up dynamo (bring all nodes online, generate the static preference list, enable the network)
	// Set the R=2 and W requirements for quorom
	// configure the timeout parameter between client and dynamo cluster

	test_count := 100
	for i := 0; i < test_count; i++ {
		key_string := fmt.Sprintf("\"Key number: %d\"", i)
		key := []byte(key_string)
		value := i
		ack := put(key, nil, value, coordinator)
		// Move actual hashing into dynamo.go
		// data := []byte("These pretzels are making me thirsty.")
		// fmt.Printf("%x\n", md5.Sum(data))
		// fmt.Printf("len: %d\n", len(md5.Sum(data)))
		// fmt.Printf("cap: %d\n", cap(md5.Sum(data)))
		if !ack {
			t.Fatalf("Failed to receive ack from dynamo cluster on put(%s, nil, %d)", key_string, value)
		}
	}

	for i := 0; i < test_count; i++ {
		key_string := fmt.Sprintf("\"Key number: %d\"", i)
		key := []byte(key_string)
		ack, value, _ := get(key)
		if ack == false {
			t.Fatalf("Failed to receive ack from dynamo cluster on get(%s)", key_string)
		}
		if len(value) != 1 {
			t.Fatalf("Received too many or too few return values: %d on get(%s)", len(value), key_string)
		}
		if value[0] != i {
			t.Fatalf("get(%s) returned value %d which does not match expected value %d", key_string, value[0], i)
		}
	}

	fmt.Printf("  ... Passed --\n")
	// fmt.Printf("  %4.1f  %d %4d %7d %4d\n", t, npeers, nrpc, nbytes, ncmds)
}

// Description: Write multiple values to dynamo.  Eventually, a coordinator node fails.
// Verify the failure detector detects the node has failed after some time, by watching the
// updates to the global preference list.
// Verify the hinted handoff -> using 4 nodes, 2 replicas, require R = 2, W = 2
func TestCoordFailure2A(t *testing.T) {
	// use 4 node, 2 replica setup
	// R = 2, W = 2

}

// Description: Write multiple values to dynamo.  Eventually, a coordinator node fails.
// Write some new values to the cluster with a failed coordinator.  Revive the coordinator node.
// Dynamo_wrapper must have a copy of the latest global preference list at all times.  Write or read
// at least one value to the revived coordinator node key group, then wait for the
// global preference list to include the revived node, before verifying the updated put values.
// Then, perform a read operation on a key that was updated on the rest of the cluster.
// Verify that stale data is not returned since R = 3 & W = 2. Verify at the node level that the coordinator
// has actually received the latest update
func TestStaleData2A(t *testing.T) {
	// *** functions implemented in dynamo_wrapper and called here ***
	// Configure total number of nodes 10. Total number of replicas = 3
	// start up dynamo (bring all nodes online, generate the static preference list, enable the network)
	// Set the R=3 and W=2 requirements for quorom
	// configure the timeout parameter between client and dynamo cluster

	test_count := 1000
	for i := 0; i < test_count; i++ {
		key_string := fmt.Sprintf("\"Key number: %d\"", i)
		key := []byte(key_string)
		value := i
		ack := put(key, nil, value)
		// Move actual hashing into dynamo.go
		// data := []byte("These pretzels are making me thirsty.")
		// fmt.Printf("%x\n", md5.Sum(data))
		// fmt.Printf("len: %d\n", len(md5.Sum(data)))
		// fmt.Printf("cap: %d\n", cap(md5.Sum(data)))
		if !ack {
			t.Fatalf("Failed to receive ack from dynamo cluster on put(%s, nil, %d)", key_string, value)
		}
	}

	for i := 0; i < test_count; i++ {
		key_string := fmt.Sprintf("\"Key number: %d\"", i)
		key := []byte(key_string)
		ack, value, _ := get(key)
		if ack == false {
			t.Fatalf("Failed to receive ack from dynamo cluster on get(%s)", key_string)
		}
		if len(value) != 1 {
			t.Fatalf("Received too many or too few return values: %d on get(%s)", len(value), key_string)
		}
		if value[0] != i {
			t.Fatalf("get(%s) returned value %d which does not match expected value %d", key_string, value[0], i)
		}
	}

	fmt.Printf("  ... Passed --\n")
	// fmt.Printf("  %4.1f  %d %4d %7d %4d\n", t, npeers, nrpc, nbytes, ncmds)
}
