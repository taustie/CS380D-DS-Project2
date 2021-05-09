package dynamo

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
	intendedNode int
	// See paper section 4.3: anytime the client's timestamp metadata does not
	// exactly match dynamo timestamp, the user must reconcile the data later

	// If the most recent state of the cart is unavailable,
	// and a user makes changes to an older version of the cart,
	// that change is still meaningful and should be preserved.
	// But at the same time it shouldn’t supersede the currently
	// unavailable state of the cart, which itself may contain
	// changes that should be preserved.
	vectorTimestamp []int
}

// To do: don't forget field names must start with capital letters!

// Implement actual hash operation here
// example
// data := []byte("These pretzels are making me thirsty.")
// fmt.Printf("%x\n", md5.Sum(data))
// fmt.Printf("len: %d\n", len(md5.Sum(data)))
// fmt.Printf("cap: %d\n", cap(md5.Sum(data)))
