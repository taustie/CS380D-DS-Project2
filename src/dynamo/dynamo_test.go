package dynamo

//
// Dynamo tests.
//

import (
	"fmt"
	"testing"
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
	// Config # nodes, # replicas, R, W
	// Start-up dynamo (bring all nodes online, generate the static preference list, enable the network)
	// Set the client to cluster and vice versa timeout parameter
	// To do: In dynamo.go need a dynamo node timeout parameters for failure detector

	test_count := 100
	for i := 0; i < test_count; i++ {
		key_string := fmt.Sprintf("\"Key number: %d\"", i)
		key := []byte(key_string)
		value := i
		var context Context
		ack := put(key, value, context)
		if !ack {
			t.Fatalf("No ack received from dynamo cluster on put(%s, nil, %d)", key_string, value)
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

// Description:
// 1) Configure: # nodes > 5, # replicas >= 5, R = all replicas, W = all replicas
// 2) Send many put requests to the Dynamo cluster using normal operating conditions (no node failures)
// 3) Send many get requests to the Dynamo cluster.
// 4) Check if all values match expected value and if any fail since a quorum must handle response.
func TestNormalMaxQuorum2A(t *testing.T) {

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
