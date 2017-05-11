// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package hyperviser

import (
	"chordkv"
	"fmt"
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"port"
	"sync"
	"time"
)

// Number of chord instances that every hyperviser instance maintains.
const defaultPort = 8888

// Maximum amount of time to wait between leader calling testConfig and
// receiving the call to start the test.
const readyTimeout time.Duration = 30 * time.Second

// Maximum amount of time to wait for RPC to return.
const rpcTimeout time.Duration = 10 * time.Second

// Store known server IP addresses. Used by test leader to coordinate.
// TODO: Update when we have real servers.
var serverIPs = map[string]bool{
	"127.0.0.1": true,
}

// ChordSet stores a bundle of chord information.
type chordSet struct {
	ch   *chordkv.Chord
	kvs  *chordkv.KVServer
	rpcs *chordkv.RPCServer
}

type testInfo struct {
	isTesting bool
	isRunning bool
	isReady   bool
	readyChan chan bool
	killChan  chan bool
	isLeader  bool
	leader    net.IP
	baseCh    *chordkv.Chord
	testType  TestType
	lg        *log.Logger
	lgBuf     io.WriteCloser
}

// Hyperviser manages...
type Hyperviser struct {
	isRunning    bool
	pass         string
	logDir       string
	mu           sync.Mutex
	ip           net.IP
	servListener net.Listener
	ti           testInfo
	chs          *[]chordSet
}

// TestType indicates the test that should be performed.
type TestType string

// TODO: More test names...
const (
	// LookupPerf measures lookup latency as function of nodes in the network.
	LookupPerf TestType = "LookupPerf"
)

type rpcEndpoint string

const (
	addOne      rpcEndpoint = "Hyperviser.AddOne"
	removeOne   rpcEndpoint = "Hyperviser.RemoveOne"
	prepareTest rpcEndpoint = "Hyperviser.PrepareTest"
	startTest   rpcEndpoint = "Hyperviser.StartTest"
	abortTest   rpcEndpoint = "Hyperviser.AbortTest"
	ready       rpcEndpoint = "Hyperviser.Ready"
	kill        rpcEndpoint = "Hyperviser.Kill"
	done        rpcEndpoint = "Hyperviser.Done"
	failed      rpcEndpoint = "Hyperviser.Failed"
)

// Call the given RPC function.
func callRPC(fname rpcEndpoint, ip string, pport int, args, reply *interface{}) error {
	// TODO: Use TLS
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", ip, pport), rpcTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	return client.Call(string(fname), args, reply)
}

// Clear all information associated with any test that could be running. THIS
// METHOD ASSUMES THAT IT IS CALLED FROM A LOCKING CONTEXT.
func (ti *testInfo) clear() {
	ti.isTesting = false
	ti.isRunning = false
	ti.isReady = false
	ti.isLeader = false
	ti.leader = nil
	ti.testType = ""
	ti.lg = nil
	ti.baseCh = nil

	if ti.readyChan != nil {
		go writeBool(ti.readyChan)
		ti.readyChan = nil
	}

	if ti.killChan != nil {
		go writeBool(ti.killChan)
		ti.killChan = nil
	}

	if hv.ti.lgBuf != nil {
		ti.lgBuf.Close()
		ti.lgBuf = nil
	}
}

// AuthArgs used validate RPC caller.
type AuthArgs struct {
	CallerIP net.IP
	Password string
}

// Write value to bool channel.
func writeBool(c chan bool) {
	c <- true
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
	if hv.ti.isTesting && !hv.ti.isLeader && !hv.ti.leader.Equal(aa.CallerIP) {
		return fmt.Errorf("Not called by leader")
	}

	return nil
}

// Shut down single chord instance. THIS METHOD ASSUMES THAT IT IS CALLED FROM
// A LOCKING CONTEXT.
func (chs *chordSet) closeChord() error {
	chSet.ch.Kill()
	return chSet.rpcs.End()
}

// Shut down all of the chord instances associated with the current test. THiS
// METHOD ASSUMES THAT IT IS CALLED FROM A LOCKING CONTEXT.
func (hv *Hyperviser) closeChords() {
	if hv.chs == nil {
		return
	}

	wg := &sync.WaitGroup()
	for _, chSet := range hv.chs {
		if chSet == nil {
			continue
		}

		wg.Add(1)
		go func(chs *chordSet) {
			defer wg.Done()
			if err := chSet.choseChord(); err != nil {
				DPrintf("hv (%s): error closing chord: %s", hv.ip.String(), err)
			}
		}(chSet)
	}
	wg.Wait()
	hv.chs = nil
}

// TestArgs used to configure information about a test.
type TestArgs struct {
	AA        *AuthArgs
	TType     TestType
	LogName   string
	NumChords int
	BaseChord *chordkv.Node
}

// PrepareTest tells hv that another hv would like to be leader for the given
// test, and it runs the test.
func (hv *Hyperviser) PrepareTest(args *TestArgs, reply *struct{}) error {
	hv.mu.Lock()
	defer hv.mu.Lock()

	if err := hv.validate(args.AA); err != nil {
		return err
	}
	if hv.ti.isTesting {
		return fmt.Errorf("Already running test %v", hv.ti.testType)
	}
	if args.NumChords <= 0 {
		return fmt.Errorf("Invalid number of Chords (%d)", args.NumChords)
	}

	hv.ti.isTesting = true
	hv.ti.isLeader = false
	hv.ti.leader = args.AA.CallerIP
	hv.ti.testType = args.TType
	hv.ti.baseCh = args.BaseChord
	hv.ti.killChan = make(chan bool)

	// TODO: file name validation
	f, err := os.Create(hv.logDir + args.LogName)
	if err != nil {
		hv.ti.clear()
		return fmt.Errorf("Open log failed: %s", err)
	}
	hv.ti.lgBuf = f
	hv.ti.lg = log.New(f, hv.ip.String(), log.LstdFlags|log.LUTC)

	// Prepare the test.
	go hv.initTest(args.NumChords)

	return nil
}

// Stop the current running test and reset state. useLocks parameter used to
// indicate whether stopTest needs to acquire locks. If useLocks set to false
// this method assumes that the calling function acquired appropriate locks.
func (hv *Hyperviser) stopTest(useLocks bool) {
	if useLocks {
		hv.mu.Lock()
		defer hv.mu.Unlock()
	}

	hv.closeChords()
	hv.ti.clear()
}

// Add a chord instance to hv using baseCh to initialize the new chord. THIS
// METHOD ASSUMES THAT IT IS CALLED FROM A LOCKING CONTEXT.
func (hv *Hyperviser) addChord(baseCh *chordkv.Chord) error {
	if baseCh == nil {
		return fmt.Errorf("addChord called with nil baseCh")
	}

	// Ensure everything is initialized.
	if hv.chs == nil {
		hv.chs = make([]chordSet, 0)
	}

	p := port.New()
	ch := chordkv.MakeChord(chordkv.MakeNode(hv.ip, p), baseCh)
	kvs := chordkv.MakeKVServer(ch)
	rpcs, err := chordkv.StartRPC(ch, kvs, p)
	if err != nil {
		ch.Kill()
		return fmt.Errorf("StartRPC failed: %s", err)
	}

	hv.chs = append(hv.chs, chordSet{ch, kvs, rpcs})
	return nil
}

// initTest configures this hyperviser instance to test.
func (hv *Hyperviser) initTest(numChords int) {
	hv.mu.Lock()
	defer hv.mu.Lock()
	if numChords <= 0 {
		DPrintf("hv (%s): initTest: invalid numChords (%d)", hv.ip.String(), numChords)
		hv.ti.clear()
		return
	}
	if hv.chs != nil {
		DPrintf("hv (%s): initTest: chord instances already exist", hv.ip.String())
	}

	for i := 0; i < numChords; i++ {
		if err := hv.addChord(hv.ti.baseCh); err != nil {
			// Initialization failed.
			DPrintf("hv (%s): initTest: addChord failed: %s", hv.ip.String(), err)
			hv.stopTest(false)
			return
		}

		// Give time to stabilize.
		time.Sleep(500 * time.Millisecond)
	}

	hv.ti.isReady = true
	hv.ti.readyChan = make(chan bool)

	// Tell leader hv is ready.
	go func() {
		args := &AuthArgs{hv.ip, hv.pass}
		var reply struct{}
		err := callRPC(ready, hv.ti.leader, defaultPort, args, reply)
		if err != nil {
			DPrintf("hv (%s): Ready failed: %s", hv.ip.String(), err)
		}
	}()

	// Goroutine to start leader response timeout
	go func() {
		select {
		case <-time.After(readyTimeout):
			// Clean up everything.
			stopTest(true)
		case <-hv.ti.killChan:
			stopTest(true)
		case <-hv.ti.readyChan:
			// Nothing to do but exit.
		}
	}()
}

// StartTest serves as a synchronization barrier for tests so that the test
// leader can ensure all hypervisers are ready.
func (hv *Hyperviser) StartTest(args *TestArgs, reply *struct{}) error {
	hv.mu.Lock()
	defer hv.mu.Lock()

	err := hv.validate(args.AA)
	switch {
	case err != nil:
		return err
	case !hv.ti.isTesting:
		return fmt.Errorf("No test running")
	case args.TType != hv.ti.testType:
		return fmt.Errorf("Invalid test type (%v)", arags.TType)
	case hv.ti.isLeader:
		return fmt.Errorf("Cannot call StartTest on leader")
	case !hv.ti.isReady:
		return fmt.Errof("Not ready to start test")
	case hv.ti.isRunning:
		return fmt.Errorf("Test already running")
	}

	hv.ti.readyChan <- true
	hv.ti.isRunning = true

	switch hv.ti.testType {
	case LookupPerf:
		go hv.lookupPerf()
	}

	return nil
}

// AbortTest allows the test leader to stop the test if something goes wrong.
func (hv *Hyperviser) AbortTest(args *AuthArgs, reply *struct{}) error {
	hv.mu.Lock()
	defer hv.mu.Unlock()
	if err := hv.verify(args); err != nil {
		return err
	}
	if hv.ti.leader {
		return fmt.Errorf("Can't call AbortTest on leader")
	}
	if !hv.ti.isTesting {
		return fmt.Errorf("No test running")
	}

	go writeBool(hw.ti.killChan)
	hv.stopTest(false)

	return nil
}

// AddOne called to have hv start a new chord instance.
func (hv *Hyperviser) AddOne(args *AuthArgs, reply *struct{}) error {
	hv.mu.Lock()
	defer hv.mu.Unlock()
	if err := hv.verify(args); err != nil {
		return err
	}
	if !hv.ti.isTesting {
		return fmt.Errorf("No test running")
	}
	if !hv.ti.isReady {
		return fmt.Errorf("Not ready")
	}

	// Only use hv.ti.baseCh if it's the only option to avoid unnecessary load
	// on the leader hyperviser.
	var baseCh *chordkv.Chord
	if len(hv.chs) > 0 {
		baseCh = hv.chs[0].ch
	} else {
		baseCh = hv.ti.baseCh
	}

	return hv.addChord(baseCh)
}

// RemoveOne called to have hv remove a chord instance.
func (hv *Hyperviser) RemoveOne(args *AuthArgs, reply *struct{}) error {
	hv.mu.Lock()
	defer hv.mu.Unlock()
	if err := hv.verify(args); err != nil {
		return err
	}
	if !hv.ti.isTesting {
		return fmt.Errorf("No test running")
	}
	if !hv.ti.isReady {
		return fmt.Errorf("Not ready")
	}
	if hv.chs == nil || len(hv.chs) == 0 {
		return fmt.Errorf("Cannot remove more chords")
	}

	i := len(hv.chs) - 1
	if err := hv.chs[i].clseChord(); err != nil {
		// TODO: if this happens, not clear how to handle state.
		return fmt.Errof("CloseChord failed: %s", err)
	}

	hv.chs = hv.chs[:i]
	return nil
}

// Ready called by test followers when they are ready to begin the test.
func (hv *Hyperviser) Ready(args *AuthArgs, reply *struct{}) error {
	return nil
}

// Done called by test followers when they are done running their test.
func (hv *Hyperviser) Done(args *AuthArgs, reply *struct{}) error {
	return nil
}

// Failed called by test followers if their test failed for some reason.
func (hv *Hyperviser) Failed(args *AuthArgs, reply *struct{}) error {
	return nil
}

// Make a new Hyperviser.
func Make(ip, pass, logDir string) (*Hyperviser, error) {
	return makeAddr(ip, pass, logDir, fmt.Sprintf(":%d", defaultPort))
}

func makeAddr(ip, pass, logDir, servIP string) (*Hyperviser, error) {
	hv := &Hyperviser{}
	hv.pass = pass
	hv.logDir = logDir // TODO: logDir validation
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

	if hv.ti.isTesting {
		hv.stopTest(false)
	}

	hv.servListener.Close()
	return nil
}

// TESTS ...

// lookupPerf measures lookup latency as a function of number of nodes in the
// chord ring.
func (hv *Hyperviser) lookupPerf() {

}
