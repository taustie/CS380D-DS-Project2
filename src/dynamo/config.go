package dynamo

//
// support for Raft tester.
//
// we will use the original config.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import "../labrpc"

import "sync"
import "testing"
import "runtime"
import "math/rand"
import crand "crypto/rand"
import "math/big"
import "encoding/base64"
import "time"
import "fmt"

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func makeSeed() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

type config struct {
	mu          sync.Mutex
	t           *testing.T
	net         *labrpc.Network
	n           int
	dynamoNodes []*Dynamo
	applyErr    []string   // from apply channel readers
	connected   []bool     // whether each server is on the net
	endnames    [][]string // the port file names each sends to
	start       time.Time  // time at which make_config() was called
	// begin()/end() statistics
	t0        time.Time // time at which test_test.go called cfg.begin()
	rpcs0     int       // rpcTotal() at start of test
	cmds0     int       // number of agreements
	bytes0    int64
	maxIndex  int
	maxIndex0 int
}

var ncpu_once sync.Once

func make_config(t *testing.T, n int, replicaCount int, quorumR int, quoromW int, unreliable bool) *config {
	ncpu_once.Do(func() {
		if runtime.NumCPU() < 2 {
			fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
		}
		rand.Seed(makeSeed())
	})
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.net = labrpc.MakeNetwork()
	// need extra for client node
	cfg.n = n + 1
	cfg.applyErr = make([]string, cfg.n)
	cfg.dynamoNodes = make([]*Dynamo, cfg.n)
	cfg.connected = make([]bool, cfg.n)
	cfg.endnames = make([][]string, cfg.n)
	cfg.start = time.Now()

	cfg.setunreliable(unreliable)

	cfg.net.LongDelays(true)

	// create a full set of Dynamo nodes.
	for i := 0; i < cfg.n; i++ {
		cfg.start1(i, replicaCount, quorumR, quoromW)
	}

	// connect everyone
	for i := 0; i < cfg.n; i++ {
		cfg.connect(i)
	}

	return cfg
}

// shut down a Raft server but save its persistent state.
func (cfg *config) crash1(i int) {
	cfg.disconnect(i)
	cfg.net.DeleteServer(i) // disable client connections to the server.
}

//
// start or re-start a Raft.
// if one already exists, "kill" it first.
// allocate new outgoing port file names, and a new
// state persister, to isolate previous instance of
// this server. since we cannot really kill it.
//
func (cfg *config) start1(i int, replicaCount int, quorumR int, quoromW int) {
	cfg.crash1(i)

	// a fresh set of outgoing ClientEnd names.
	// so that old crashed instance's ClientEnds can't send.
	cfg.endnames[i] = make([]string, cfg.n)
	// add 1 for client RPCs
	for j := 0; j < cfg.n; j++ {
		cfg.endnames[i][j] = randstring(20)
	}

	// a fresh set of ClientEnds.
	ends := make([]*labrpc.ClientEnd, cfg.n)
	for j := 0; j < cfg.n; j++ {
		ends[j] = cfg.net.MakeEnd(cfg.endnames[i][j])
		cfg.net.Connect(cfg.endnames[i][j], j)
	}

	// listen to messages from Raft indicating newly committed messages.
	// applyCh := make(chan ApplyMsg)
	// go func() {
	// 	for m := range applyCh {
	// 		err_msg := ""
	// 		if m.CommandValid == false {
	// 			// ignore other types of ApplyMsg
	// 		} else {
	// 			v := m.Command
	// 			cfg.mu.Lock()
	// 			for j := 0; j < len(cfg.logs); j++ {
	// 				if old, oldok := cfg.logs[j][m.CommandIndex]; oldok && old != v {
	// 					// some server has already committed a different value for this entry!
	// 					err_msg = fmt.Sprintf("commit index=%v server=%v %v != server=%v %v",
	// 						m.CommandIndex, i, m.Command, j, old)
	// 				}
	// 			}
	// 			_, prevok := cfg.logs[i][m.CommandIndex-1]
	// 			cfg.logs[i][m.CommandIndex] = v
	// 			if m.CommandIndex > cfg.maxIndex {
	// 				cfg.maxIndex = m.CommandIndex
	// 			}
	// 			cfg.mu.Unlock()
	//
	// 			if m.CommandIndex > 1 && prevok == false {
	// 				err_msg = fmt.Sprintf("server %v apply out of order %v", i, m.CommandIndex)
	// 			}
	// 		}
	//
	// 		if err_msg != "" {
	// 			log.Fatalf("apply error: %v\n", err_msg)
	// 			cfg.applyErr[i] = err_msg
	// 			// keep reading after error so that Raft doesn't block
	// 			// holding locks...
	// 		}
	// 	}
	// }()

	rf := Make(ends, i, replicaCount, quorumR, quoromW)

	cfg.mu.Lock()
	cfg.dynamoNodes[i] = rf
	cfg.mu.Unlock()

	svc := labrpc.MakeService(rf)
	srv := labrpc.MakeServer()
	srv.AddService(svc)
	cfg.net.AddServer(i, srv)
}

func (cfg *config) checkTimeout() {
	// enforce a two minute real-time limit on each test
	if !cfg.t.Failed() && time.Since(cfg.start) > 120*time.Second {
		cfg.t.Fatal("test took longer than 120 seconds")
	}
}

func (cfg *config) cleanup() {
	for i := 0; i < len(cfg.dynamoNodes); i++ {
		if cfg.dynamoNodes[i] != nil {
			cfg.dynamoNodes[i].Kill()
		}
	}
	cfg.net.Cleanup()
	cfg.checkTimeout()
}

// attach server i to the net.
func (cfg *config) connect(i int) {
	// fmt.Printf("connect(%d)\n", i)

	cfg.connected[i] = true

	// outgoing ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.connected[j] {
			endname := cfg.endnames[i][j]
			cfg.net.Enable(endname, true)
		}
	}

	// incoming ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.connected[j] {
			endname := cfg.endnames[j][i]
			cfg.net.Enable(endname, true)
		}
	}
}

// detach server i from the net.
func (cfg *config) disconnect(i int) {
	// fmt.Printf("disconnect(%d)\n", i)

	cfg.connected[i] = false

	// outgoing ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.endnames[i] != nil {
			endname := cfg.endnames[i][j]
			cfg.net.Enable(endname, false)
		}
	}

	// incoming ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.endnames[j] != nil {
			endname := cfg.endnames[j][i]
			cfg.net.Enable(endname, false)
		}
	}
}

func (cfg *config) rpcCount(server int) int {
	return cfg.net.GetCount(server)
}

func (cfg *config) rpcTotal() int {
	return cfg.net.GetTotalCount()
}

func (cfg *config) setunreliable(unrel bool) {
	cfg.net.Reliable(!unrel)
}

func (cfg *config) bytesTotal() int64 {
	return cfg.net.GetTotalBytes()
}

func (cfg *config) setlongreordering(longrel bool) {
	cfg.net.LongReordering(longrel)
}

// start a Test.
// print the Test message.
// e.g. cfg.begin("Test (2B): RPC counts aren't too high")
func (cfg *config) begin(description string) {
	fmt.Printf("%s ...\n", description)
	cfg.t0 = time.Now()
	cfg.rpcs0 = cfg.rpcTotal()
	cfg.bytes0 = cfg.bytesTotal()
	cfg.cmds0 = 0
	cfg.maxIndex0 = cfg.maxIndex
}

// end a Test -- the fact that we got here means there
// was no failure.
// print the Passed message,
// and some performance numbers.
func (cfg *config) end() {
	cfg.checkTimeout()
	if cfg.t.Failed() == false {
		cfg.mu.Lock()
		t := time.Since(cfg.t0).Seconds()       // real time
		npeers := cfg.n                         // number of Raft peers
		nrpc := cfg.rpcTotal() - cfg.rpcs0      // number of RPC sends
		nbytes := cfg.bytesTotal() - cfg.bytes0 // number of bytes
		ncmds := cfg.maxIndex - cfg.maxIndex0   // number of Raft agreements reported
		cfg.mu.Unlock()

		fmt.Printf("  ... Passed --")
		fmt.Printf("  %4.1f  %d %4d %7d %4d\n", t, npeers, nrpc, nbytes, ncmds)
	}
}
