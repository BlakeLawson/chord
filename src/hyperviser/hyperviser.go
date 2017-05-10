// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package hyperviser

import (
	"chordkv"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// Number of chord instances that every hyperviser instance maintains.
const defaultPort = 8888

// Maximum amount of time to wait between leader calling testConfig and
// receiving the call to start the test.
const testTimeout time.Duration = 30 * time.Second

// Maximum amount of time to wait for RPC to return.
const rpcTimeout time.Duration = 10 * time.Second

// Store known server IP addresses. Used by test leader to coordinate.
var serverIPs map[string]bool = map[string]bool {
	"127.0.0.1": true,
}

// ChordSet stores a bundle of chord information.
type chordSet struct {
	kvs *chordkv.KVServer
	rpcs *chordkv.RPCServer
}

// Hyperviser manages...
type Hyperviser struct {
	pass string
	isTesting bool
	isRunning bool
	isLeader bool
	leader net.IP
	mu sync.Mutex
	servListener net.Listener
	ip net.IP
	lg *log.Logger
	chs *[]chordSet
}

// TestType indicates the test that should be performed.
// TODO: More test names...
type TestType string
const (
	// LookupPerf measures lookup latency as function of nodes in the network.
	LookupPerf TestType = "LookupPerf"
)

type rpcEndpoint string
const (
	addOne rpcEndpoint = "Hyperviser.AddOne"
	removeOne rpcEndpoint = "Hyperviser.RemoveOne"
	test rpcEndpoint = "Hyperviser.Test"
	startTest rpcEndpoint = "Hyperviser.StartTest"
	abortTest rpcEndpoint = "Hyperviser.AbortTest"
	ready rpcEndpoint = "Hyperviser.Ready"
	kill rpcEndpoint = "Hyperviser.Kill"
)

// Call the given RPC function.
func callRPC(fname rpcEndpoint, ip string, port int, args, reply *interface{}) error {
	// TODO: Use TLS
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", ip, port), rpcTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	return client.Call(string(fname), args, reply)
}

// AuthArgs used validate RPC caller.
type AuthArgs struct {
	CallerIP net.IP
	Password string
}

// validate the IP address given to an RPC endpoint. THIS METHOD ASSUMES THAT
// IT IS CALLED FROM A LOCKING CONTEXT.
func (hv *Hyperviser) validate(aa *AuthArgs) error {
	if _, ok := serverIPs[aa.CallerIP.String()]; !ok {
		return fmt.Errorf("Invalid IP address")
	}
	if aa.Password != hv.pass {
		return fmt.Errorf("Invalid password")
	}
	if hv.isTesting && !hv.isLeader && !hv.leader.Equal(aa.CallerIP) {
		return fmt.Errorf("Not called by leader")
	}

	return nil
}

// initTest configures this hyperviser instance to test.
func (hv *Hyperviser) initTest(numChord int) error {
	return nil
}

// AddOne called to have hv start a new chord instance.
func (hv *Hyperviser) AddOne(args *AuthArgs, reply *struct{}) error {
	return nil
}

// RemoveOne called to have hv remove a chord instance.
func (hv *Hyperviser) RemoveOne(args *AuthArgs, reply *struct{}) error {
	return nil
}

// TestArgs used to configure information about a test.
type TestArgs struct {
	AA AuthArgs
	TType TestType
	LogName string
	NumChords int
}

// Test tells hv that another hv would like to be leader for the given test,
// and it runs the test.
func (hv *Hyperviser) Test(args *TestArgs, reply *struct{}) error {
	return nil
}

// StartTest serves as a synchronization barrier for tests so that the test
// leader can ensure all hypervisers are ready.
func (hv *Hyperviser) StartTest(args *TestArgs, reply *struct{}) error {
	return nil
}

// AbortTest allows the test leader to stop the test if something goes wrong.
func (hv *Hyperviser) AbortTest(args *AuthArgs, reply *struct{}) error {
	return nil
}

// Ready called by test followers when they are ready to begin the test.
func (hv *Hyperviser) Ready(args *AuthArgs, reply *struct{}) error {
	return nil
}

// Make a new Hyperviser.
func Make(ip, pass string) (*Hyperviser, error) {
	return makeSetAddr(ip, pass, fmt.Sprintf(":%d", defaultPort))
}

func makeSetAddr(ip, pass, servIP string) (*Hyperviser, error) {
	hv := &Hyperviser{}
	hv.pass = pass
	hv.ip = net.ParseIP(ip)
	if hv.ip == nil {
		return nil, fmt.Errorf("Invalid IP address")
	}

	// TODO: Add TLS
	var err error
	hv.servListener, err = net.Listen("tcp", servIP)
	if err != nil {
		return nil, err
	}

	serv := rpc.NewServer()
	if err := serv.Register(hv); err != nil {
		return nil, fmt.Errorf("rpc registration failed: %s", err)
	}

	hv.isRunning = true
	go func() {
		// Blocks until completion
		serv.Accept(hv.servListener)

		hv.mu.Lock()
		hv.isRunning = false
		hv.mu.Unlock()
	}()

	return hv, nil
}

// Kill shuts down the Hyperviser.
func (hv *Hyperviser) Kill(args *AuthArgs, reply *struct{}) error {
	hv.mu.Lock()
	defer hv.mu.Unlock()
	if !hv.isRunning {
		return nil
	}

	if hv.isTesting {
		// TODO: extra clean up
	}

	hv.servListener.Close()
	return nil
}
