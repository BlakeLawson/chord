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
var serverIPs = map[string]bool{}

// ChordSet stores a bundle of chord information.
type chordSet struct {
	ch   *chordkv.Chord
	kvs  *chordkv.KVServer
	rpcs *chordkv.RPCServer
}

type testInfo struct {
	tNum      int
	isTesting bool
	isRunning bool
	isReady   bool
	readyChan chan bool
	killChan  chan bool
	isLeader  bool
	leader    net.IP
	baseCh    *chordkv.Node
	testType  TestType
	lg        *log.Logger
	lgBuf     io.WriteCloser
}

// leaderState used by leader to maintain information about the state of the
// test it's leading.
type leaderState struct {
	mu      sync.Mutex
	readyWg *sync.WaitGroup
	doneWg  *sync.WaitGroup
	servers *map[string]*serverInfo
	lg      *log.Logger
	lgBuf   io.WriteCloser
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
	chs          *[]*chordSet
	ls           *leaderState
}

type rpcEndpoint string

const (
	getStatusRPC   rpcEndpoint = "Hyperviser.GetStatus"
	addChordRPC    rpcEndpoint = "Hyperviser.AddChord"
	removeChordRPC rpcEndpoint = "Hyperviser.RemoveChord"
	prepareTestRPC rpcEndpoint = "Hyperviser.PrepareTest"
	startTestRPC   rpcEndpoint = "Hyperviser.StartTest"
	abortTestRPC   rpcEndpoint = "Hyperviser.AbortTest"
	readyRPC       rpcEndpoint = "Hyperviser.Ready"
	killRPC        rpcEndpoint = "Hyperviser.Kill"
	doneRPC        rpcEndpoint = "Hyperviser.Done"
	failedRPC      rpcEndpoint = "Hyperviser.Failed"
)

// Call the given RPC function.
func callRPC(fname rpcEndpoint, ip string, pport int, timeout time.Duration,
	args, reply interface{}) error {
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

	if ti.lgBuf != nil {
		ti.lgBuf.Close()
		ti.lgBuf = nil
	}
}

// AuthArgs used validate RPC caller.
type AuthArgs struct {
	CallerIP net.IP
	Password string
}

func (hv *Hyperviser) makeAuthArgs() *AuthArgs {
	return &AuthArgs{hv.ip, hv.pass}
}

// Write value to bool channel.
func writeBool(c chan bool) {
	c <- true
}

// validate the IP address given to an RPC endpoint. THIS METHOD ASSUMES THAT
// IT IS CALLED FROM A LOCKING CONTEXT.
func (hv *Hyperviser) validate(aa *AuthArgs) error {
	if !hv.isRunning {
		return fmt.Errorf("Not running", hv.ip.String())
	}
	if _, ok := serverIPs[aa.CallerIP.String()]; !ok {
		return fmt.Errorf("Invalid IP address")
	}
	if aa.Password != hv.pass {
		return fmt.Errorf("Invalid password")
	}
	if hv.ti.isTesting && !hv.ti.isLeader && !hv.ti.leader.Equal(aa.CallerIP) {
		return fmt.Errorf("Not called by leader")
	}
	if hv.ti.isTesting && hv.ti.isLeader {
		hv.ls.mu.Lock()
		defer hv.ls.mu.Unlock()
		if _, ok := (*hv.ls.servers)[aa.CallerIP.String()]; !ok {
			return fmt.Errorf("Not part of test")
		}
	}

	return nil
}

// Shut down single chord instance. THIS METHOD ASSUMES THAT IT IS CALLED FROM
// A LOCKING CONTEXT.
func (chs *chordSet) closeChord() error {
	chs.ch.Kill()
	return chs.rpcs.End()
}

// Shut down all of the chord instances associated with the current test. THiS
// METHOD ASSUMES THAT IT IS CALLED FROM A LOCKING CONTEXT.
func (hv *Hyperviser) closeChords() {
	if hv.chs == nil {
		return
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < len(*hv.chs); i++ {
		if (*hv.chs)[i] == nil {
			continue
		}

		wg.Add(1)
		go func(chs *chordSet) {
			defer wg.Done()
			if err := chs.closeChord(); err != nil {
				DPrintf("hv (%s): error closing chord: %s", hv.ip.String(), err)
			}
		}((*hv.chs)[i])
	}
	wg.Wait()
	hv.chs = nil
}

// TestArgs used to configure information about a test.
type TestArgs struct {
	AA            *AuthArgs
	TType         TestType
	LogName       string
	NumChords     int
	BaseChordNode *chordkv.Node
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
	if _, ok := tests[args.TType]; !ok {
		return fmt.Errorf("Invalid test type: %s", args.TType)
	}

	hv.ti.isTesting = true
	hv.ti.isLeader = false
	hv.ti.leader = args.AA.CallerIP
	hv.ti.testType = args.TType
	hv.ti.baseCh = args.BaseChordNode
	hv.ti.killChan = make(chan bool)

	// TODO: file name validation
	f, err := os.Create(hv.logDir + args.LogName)
	if err != nil {
		hv.ti.clear()
		return fmt.Errorf("Open log failed: %s", err)
	}
	hv.ti.lgBuf = f
	hv.ti.lg = log.New(f, hv.ip.String(), log.LstdFlags|log.LUTC)
	hv.ti.tNum++

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
func (hv *Hyperviser) addChord(baseChNode *chordkv.Node) error {
	// Ensure everything is initialized.
	if hv.chs == nil {
		hv.chs = new([]*chordSet)
	}

	p, err := port.New()
	if err != nil {
		return fmt.Errorf("port.New failed: %s", err)
	}

	ch, err := chordkv.MakeChord(chordkv.MakeNode(hv.ip, p), baseChNode)
	if err != nil {
		return fmt.Errorf("MakeChord failed: %s", err)
	}

	kvs := chordkv.MakeKVServer(ch)
	rpcs, err := chordkv.StartRPC(ch, kvs, p)
	if err != nil {
		ch.Kill()
		return fmt.Errorf("StartRPC failed: %s", err)
	}

	*hv.chs = append(*hv.chs, &chordSet{ch, kvs, rpcs})
	time.Sleep(time.Second)
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
	}

	hv.ti.isReady = true
	hv.ti.readyChan = make(chan bool)

	// Tell leader hv is ready.
	go func() {
		args := &AuthArgs{hv.ip, hv.pass}
		var reply struct{}
		err := callRPC(readyRPC, hv.ti.leader.String(), defaultPort, rpcTimeout, args, reply)
		if err != nil {
			DPrintf("hv (%s): Ready failed: %s", hv.ip.String(), err)
		}
	}()

	// Goroutine to start leader response timeout
	go func() {
		select {
		case <-time.After(readyTimeout):
			// Clean up everything.
			hv.stopTest(true)
		case <-hv.ti.killChan:
			hv.stopTest(true)
		case <-hv.ti.readyChan:
			// Nothing to do but exit.
		}
	}()
}

func (hv *Hyperviser) executeTest(tt TestType, tNum int) {
	err := tests[tt].f(hv)
	hv.mu.Lock()
	if hv.ti.tNum != tNum {
		// Test changed somehow.
		DPrintf("hv (%s): test changed from %d -> %d during test %d",
			hv.ip.String(), tNum, hv.ti.tNum, tNum)
		return
	}

	ti := hv.ti
	hv.mu.Unlock()

	var reply struct{}
	if err != nil {
		args := FailArgs{hv.makeAuthArgs(), err}
		err = callRPC(failedRPC, ti.leader.String(), defaultPort, rpcTimeout, args, &reply)
		DPrintf("hv (%s): executeTest failed: %s", hv.ip.String(), err)
	} else {
		args := hv.makeAuthArgs()
		err = callRPC(doneRPC, ti.leader.String(), defaultPort, rpcTimeout, args, &reply)
	}

	if err != nil {
		hv.ti.lg.Printf("hv (%s): executeTest: RPC failed: %s", hv.ip.String(), err)
	}

	hv.stopTest(true)
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
		return fmt.Errorf("Invalid test type (%v)", args.TType)
	case hv.ti.isLeader:
		return fmt.Errorf("Cannot call StartTest on leader")
	case !hv.ti.isReady:
		return fmt.Errorf("Not ready to start test")
	case hv.ti.isRunning:
		return fmt.Errorf("Test already running")
	}

	hv.ti.readyChan <- true
	hv.ti.isRunning = true

	go hv.executeTest(hv.ti.testType, hv.ti.tNum)
	return nil
}

// AbortTest allows the test leader to stop the test if something goes wrong.
func (hv *Hyperviser) AbortTest(args *AuthArgs, reply *struct{}) error {
	hv.mu.Lock()
	defer hv.mu.Unlock()
	if err := hv.validate(args); err != nil {
		return err
	}
	if hv.ti.isLeader {
		return fmt.Errorf("Can't call AbortTest on leader")
	}
	if !hv.ti.isTesting {
		return fmt.Errorf("No test running")
	}

	go writeBool(hv.ti.killChan)
	hv.stopTest(false)

	return nil
}

// RingModArgs used to change number of chord instances on a Hyperviser.
type RingModArgs struct {
	AA *AuthArgs
	N  int
}

// AddChord called to have hv start a new chord instance.
func (hv *Hyperviser) AddChord(args *RingModArgs, reply *struct{}) error {
	hv.mu.Lock()
	defer hv.mu.Unlock()
	if err := hv.validate(args.AA); err != nil {
		return err
	}
	if args.N <= 0 {
		return fmt.Errorf("N invalid: %d", args.N)
	}
	if !hv.ti.isTesting {
		return fmt.Errorf("No test running")
	}
	if !hv.ti.isReady {
		return fmt.Errorf("Not ready")
	}

	// Only use hv.ti.baseCh if it's the only option to avoid unnecessary load
	// on the leader hyperviser.
	var baseChNode *chordkv.Node
	if len(*hv.chs) > 0 {
		n := (*hv.chs)[0].ch.GetNode()
		baseChNode = &n
	} else {
		baseChNode = hv.ti.baseCh
	}

	for i := 0; i < args.N; i++ {
		if err := hv.addChord(baseChNode); err != nil {
			return err
		}
	}

	return nil
}

// RemoveChord called to have hv remove a chord instance.
func (hv *Hyperviser) RemoveChord(args *RingModArgs, reply *struct{}) error {
	hv.mu.Lock()
	defer hv.mu.Unlock()
	if err := hv.validate(args.AA); err != nil {
		return err
	}
	if args.N <= 0 {
		return fmt.Errorf("N invalid: %d", args.N)
	}
	if !hv.ti.isTesting {
		return fmt.Errorf("No test running")
	}
	if !hv.ti.isReady {
		return fmt.Errorf("Not ready")
	}
	if hv.chs == nil || len(*hv.chs) == 0 {
		return fmt.Errorf("Cannot remove more chords")
	}

	// TODO: Better way to write this
	for i := 0; i < args.N; i++ {
		j := len(*hv.chs) - 1
		if j < 0 {
			return fmt.Errorf("Cannot remove more chords: %d", i)
		}

		if err := (*hv.chs)[j].closeChord(); err != nil {
			// TODO: if this happens, not clear how to handle state.
			return fmt.Errorf("CloseChord failed: %s", err)
		}

		*hv.chs = (*hv.chs)[:j]
	}

	return nil
}

// Status contains basic information about the state of a Hyperviser.
type Status struct {
	IsRunning    bool
	IsTestConfig bool
	IsReady      bool
	IsTesting    bool
	IsLeader     bool
	Leader       net.IP
	TType        TestType
}

// GetStatus returns the status of this hyperviser.
func (hv *Hyperviser) GetStatus(args *AuthArgs, reply *Status) error {
	hv.mu.Lock()
	defer hv.mu.Unlock()
	if err := hv.validate(args); err != nil {
		return err
	}

	reply.IsRunning = hv.isRunning
	reply.IsTestConfig = hv.ti.isTesting
	reply.IsReady = hv.ti.isReady
	reply.IsTesting = hv.ti.isRunning
	reply.IsLeader = hv.ti.isLeader
	reply.Leader = hv.ti.leader
	reply.TType = hv.ti.testType
	return nil
}

// serverStatus used to track state of servers during test.
type serverStatus string

const (
	uninitializedSt serverStatus = "uninitialized"
	unresponsiveSt  serverStatus = "unresponsive"
	failedSt        serverStatus = "failed"
	readySt         serverStatus = "ready"
	preparingSt     serverStatus = "preparing"
	testingSt       serverStatus = "testing"
	doneSt          serverStatus = "done"
)

// serverInfo stores information about servers
type serverInfo struct {
	status       serverStatus
	numChs       int
	targetNumChs int
	readyChan    chan bool
	doneChan     chan bool
	errChan      chan error
}

// initServers returns set of servers to use for test. Determines how many
// chord instances each server should run. Guarantees that hv is part of the
// set of servers.
func (hv *Hyperviser) initServers(targetNodes int) *map[string]*serverInfo {
	servers := make(map[string]*serverInfo)

	nodesPerServer := len(serverIPs) / targetNodes
	nodesRemaining := len(serverIPs) % targetNodes

	servers[hv.ip.String()] = &serverInfo{
		status:       uninitializedSt,
		numChs:       0,
		targetNumChs: nodesPerServer,
		readyChan:    make(chan bool),
		doneChan:     make(chan bool),
		errChan:      make(chan error)}

	var count int
	if nodesPerServer == 0 {
		count = nodesRemaining - 1
	} else {
		count = len(serverIPs) - 1
	}

	// Initialize serverInfos
	for ip := range serverIPs {
		if count == 0 {
			break
		}
		if ip == hv.ip.String() {
			continue
		}

		servers[ip] = &serverInfo{
			status:       uninitializedSt,
			numChs:       0,
			targetNumChs: nodesPerServer,
			readyChan:    make(chan bool),
			doneChan:     make(chan bool),
			errChan:      make(chan error)}
		count++
	}

	// Assign remaining nodes.
	for _, info := range servers {
		if nodesRemaining == 0 {
			break
		}
		info.targetNumChs++
		nodesRemaining--
	}

	return &servers
}

// cleanLeader resets state of leader after test execution completes. If
// useParentLock is true, acquire hv.mu.Lock before running. Otherwise, assume
// that lock has already been acquired.
func (hv *Hyperviser) cleanLeader(useParentLock bool) {
	if useParentLock {
		hv.mu.Lock()
		defer hv.mu.Unlock()
	}

	if hv.ls == nil {
		return
	}

	hv.ls.mu.Lock()
	defer hv.ls.mu.Unlock()

	hv.ls.readyWg = nil
	hv.ls.doneWg = nil

	for ip, info := range *hv.ls.servers {
		if info.status != testingSt {
			continue
		}

		go func(ipp string) {
			args := hv.makeAuthArgs()
			var reply struct{}
			err := callRPC(abortTestRPC, ipp, defaultPort, rpcTimeout, args, &reply)
			if err != nil {
				DPrintf("hv (%s): cleanLeader: abort %s failed: %s",
					hv.ip.String(), ipp, err)
			}
		}(ip)
	}
	hv.ls.servers = nil

	if hv.ls.lg != nil {
		hv.ls.lgBuf.Close()
		hv.ls.lg = nil
		hv.ls.lgBuf = nil
	}
}

// sendLeaderReady used to send Prepare message in its own thread.
func (hv *Hyperviser) sendPrepare(ip string, info *serverInfo, logName string) {
	args := &TestArgs{
		AA:            hv.makeAuthArgs(),
		TType:         hv.ti.testType,
		LogName:       logName,
		NumChords:     info.targetNumChs,
		BaseChordNode: hv.ti.baseCh}
	var reply struct{}

	timeout := time.Duration(info.targetNumChs)*time.Second + 5
	err := callRPC(prepareTestRPC, ip, defaultPort, timeout, args, &reply)

	hv.mu.Lock()
	hv.ls.mu.Lock()
	if err != nil {
		hv.ls.lg.Printf("sendPrepare: RPC to %s failed: %s",
			hv.ip.String(), ip, err)
		info.status = unresponsiveSt
	} else {
		info.status = preparingSt
		info.numChs = info.targetNumChs
	}
	hv.ls.mu.Unlock()
	hv.mu.Unlock()
}

// prepareLeader used to initialize leader in goroutine.
func (hv *Hyperviser) prepareLeader() {
	info := (*hv.ls.servers)[hv.ip.String()]
	for i := 1; i < info.targetNumChs; i++ {
		if err := hv.addChord(hv.ti.baseCh); err != nil {
			hv.ls.lg.Printf("prepareLeader: initChord failed: %s", err)
			return
		}
	}

	hv.ls.readyWg.Done()
	info.numChs = info.targetNumChs
}

// sendStart used to call start test in other thread.
func (hv *Hyperviser) sendStart(ip string, info *serverInfo, logName string) {
	args := TestArgs{
		AA:            hv.makeAuthArgs(),
		TType:         hv.ti.testType,
		LogName:       logName,
		NumChords:     info.targetNumChs,
		BaseChordNode: hv.ti.baseCh}
	var reply struct{}
	err := callRPC(startTestRPC, ip, defaultPort, rpcTimeout, args, &reply)

	hv.mu.Lock()
	hv.ls.mu.Lock()
	if err != nil {
		hv.ls.lg.Printf("sendStart: RPC to %s failed: %s", ip, err)
		info.status = unresponsiveSt
	} else {
		info.status = testingSt
	}
	hv.mu.Unlock()
	hv.ls.mu.Unlock()
}

// startLeaderTest runs current test on leader.
func (hv *Hyperviser) startLeaderTest() {
	err := tests[hv.ti.testType].f(hv)

	hv.ls.mu.Lock()
	info := (*hv.ls.servers)[hv.ip.String()]
	if err != nil {
		hv.ls.lg.Printf("Node %s test failed: %s", hv.ip.String(), err)
		info.status = failedSt
	} else {
		info.status = doneSt
	}

	hv.ls.doneWg.Done()
	hv.ls.mu.Unlock()
}

// StartLeader runs the given test as the leader.
func (hv *Hyperviser) StartLeader(testType TestType, leaderLog, testLog string) error {
	hv.mu.Lock()
	if !hv.isRunning {
		hv.mu.Unlock()
		return fmt.Errorf("Not running")
	}
	if hv.ti.isTesting {
		hv.mu.Unlock()
		return fmt.Errorf("Already testing")
	}
	if _, ok := tests[testType]; !ok {
		hv.mu.Unlock()
		return fmt.Errorf("Invalid test type: %s", testType)
	}

	// Initialize leader state
	hv.ti.isTesting = true
	hv.ti.isLeader = true
	hv.ti.killChan = make(chan bool)
	hv.ti.readyChan = make(chan bool)
	hv.ti.testType = testType

	// It is safe to unlock here because RPC calls use isLeader and isReady to
	// determine whether to run.
	hv.mu.Unlock()
	defer hv.stopTest(true)

	hv.ls = &leaderState{}
	defer hv.cleanLeader(true)

	// Set up log files.
	f, err := os.Create(hv.logDir + leaderLog)
	if err != nil {
		return fmt.Errorf("Creating leader log failed: %s", err)
	}
	hv.ls.lgBuf = f
	hv.ls.lg = log.New(f, hv.ip.String(), log.LstdFlags|log.LUTC)

	f, err = os.Create(hv.logDir + testLog)
	if err != nil {
		return fmt.Errorf("Creating test log failed: %s", err)
	}
	hv.ti.lgBuf = f
	hv.ti.lg = log.New(f, hv.ip.String(), log.LstdFlags|log.LUTC)

	// Run the test
	// TODO: new log names for each batch
	// TODO: should rewrite to reuse existing chord instances rather than
	// tearing down every time.
	for batchNum, batchSize := range tests[testType].phases {
		hv.ls.lg.Printf("Starting %s round %d", testType, batchNum)
		hv.ls.servers = hv.initServers(batchSize)

		// Initialize leader's chords
		if err := hv.addChord(nil); err != nil {
			return fmt.Errorf("First chord initialization failed: %s", err)
		}

		n := (*hv.chs)[0].ch.GetNode()
		hv.ti.baseCh = &n

		info := (*hv.ls.servers)[hv.ip.String()]
		info.numChs = 1

		// Prepare test on followers and leader
		hv.ls.lg.Println("Preparing for test")
		hv.ls.readyWg = &sync.WaitGroup{}

		hv.mu.Lock()
		hv.ti.isReady = true
		hv.mu.Unlock()
		for ip, info := range *hv.ls.servers {
			hv.ls.readyWg.Add(1)
			if ip == hv.ip.String() {
				continue
			}
			go hv.sendPrepare(ip, info, testLog)
		}
		go hv.prepareLeader()

		// Gather responses
		hv.ls.lg.Println("Waiting for ready")
		waitChan := make(chan bool)
		go func() {
			hv.ls.readyWg.Wait()
			waitChan <- true
		}()
		select {
		case <-time.After(readyTimeout):
			msg := "Timeout before servers ready"
			hv.ls.lg.Println(msg)
			return fmt.Errorf(msg)
		case <-waitChan:
			// Nothing to do but move on.
		}

		// Run test
		hv.mu.Lock()
		hv.ti.isRunning = true
		hv.mu.Unlock()
		hv.ls.lg.Println("Executing test")
		for ip, info := range *hv.ls.servers {
			hv.ls.doneWg.Add(1)
			if ip == hv.ip.String() {
				continue
			}
			go hv.sendStart(ip, info, testLog)
		}
		go hv.startLeaderTest()

		hv.ls.lg.Println("Waiting for test to finish")
		waitChan = make(chan bool)
		go func() {
			hv.ls.doneWg.Wait()
			waitChan <- true
		}()
		select {
		case <-time.After(tests[testType].timeout):
			msg := "Timeout before getting test results"
			hv.ls.lg.Println(msg)
			return fmt.Errorf(msg)
		case <-waitChan:
			// Nothing to do but move on
		}

		hv.ls.lg.Printf("Test %d Succeeded", batchNum)
	}

	return nil
}

// Ready called by test followers when they are ready to begin the test.
func (hv *Hyperviser) Ready(args *AuthArgs, reply *struct{}) error {
	hv.mu.Lock()
	defer hv.mu.Unlock()
	if err := hv.validate(args); err != nil {
		return err
	}
	if !hv.ti.isLeader {
		return fmt.Errorf("Not leader")
	}
	if !hv.ti.isReady {
		return fmt.Errorf("Not ready")
	}

	hv.ls.mu.Lock()
	defer hv.ls.mu.Unlock()

	info := (*hv.ls.servers)[args.CallerIP.String()]
	if info.status != preparingSt {
		return fmt.Errorf("Called at wrong time: %s", info.status)
	}

	info.status = readySt
	hv.ls.doneWg.Done()
	return nil
}

// Done called by test followers when they are done running their test.
func (hv *Hyperviser) Done(args *AuthArgs, reply *struct{}) error {
	hv.mu.Lock()
	defer hv.mu.Unlock()
	if err := hv.validate(args); err != nil {
		return err
	}
	if !hv.ti.isLeader {
		return fmt.Errorf("Not leader")
	}
	if !hv.ti.isReady {
		return fmt.Errorf("Not ready")
	}
	if !hv.ti.isRunning {
		return fmt.Errorf("Not running test")
	}

	hv.ls.mu.Lock()
	defer hv.ls.mu.Unlock()

	info := (*hv.ls.servers)[args.CallerIP.String()]
	if info.status != testingSt {
		return fmt.Errorf("Called at wrong time: %s", info.status)
	}

	info.status = doneSt
	hv.ls.doneWg.Done()
	return nil
}

// FailArgs contains reason why test failed.
type FailArgs struct {
	AA     *AuthArgs
	Reason error
}

// Failed called by test followers if their test failed for some reason.
func (hv *Hyperviser) Failed(args *FailArgs, reply *struct{}) error {
	hv.mu.Lock()
	defer hv.mu.Unlock()
	if err := hv.validate(args.AA); err != nil {
		return err
	}
	if !hv.ti.isLeader {
		return fmt.Errorf("Not leader")
	}
	if !hv.ti.isReady {
		return fmt.Errorf("Not ready")
	}
	if !hv.ti.isRunning {
		return fmt.Errorf("Not running test")
	}

	hv.ls.mu.Lock()
	defer hv.ls.mu.Unlock()

	info := (*hv.ls.servers)[args.AA.CallerIP.String()]
	if info.status != testingSt {
		return fmt.Errorf("Called at wrong time: %s", info.status)
	}

	hv.ls.lg.Printf("Node %s test failed: %s",
		args.AA.CallerIP.String(), args.Reason)
	info.status = failedSt
	hv.ls.doneWg.Done()
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

// Stop Hyperviser.
func (hv *Hyperviser) Stop(useLocks bool) {
	if useLocks {
		hv.mu.Lock()
		defer hv.mu.Unlock()
	}

	if !hv.isRunning {
		return
	}

	if hv.ti.isLeader {
		hv.cleanLeader(false)
	}

	if hv.ti.isTesting {
		hv.stopTest(false)
	}

	hv.servListener.Close()
}

// Kill shuts down the Hyperviser.
func (hv *Hyperviser) Kill(args *AuthArgs, reply *struct{}) error {
	hv.mu.Lock()
	defer hv.mu.Unlock()
	if err := hv.validate(args); err != nil {
		return err
	}

	hv.Stop(false)
	return nil
}

// TESTS ...

// TestType indicates the test that should be performed.
type TestType string

// TODO: More test names...
const (
	// LookupPerf measures lookup latency as function of nodes in the network.
	LookupPerf TestType = "LookupPerf"
	HelloWorld TestType = "HelloWorld"
)

type testConfig struct {
	phases  []int
	f       func(*Hyperviser) error
	timeout time.Duration
}

var tests = map[TestType]testConfig{
	LookupPerf: testConfig{
		phases:  []int{10, 30, 60, 90, 120, 150, 180, 200},
		f:       lookupPerf,
		timeout: time.Minute},
	HelloWorld: testConfig{
		phases:  []int{1, 20},
		f:       helloWorld,
		timeout: 10 * time.Second},
}

// lookupPerf measures lookup latency as a function of number of nodes in the
// chord ring.
func lookupPerf(hv *Hyperviser) error {
	return nil
}

// helloWorld used to test the hyperviser framework. Plz work
func helloWorld(hv *Hyperviser) error {
	hv.ti.lg.Println("Hello World")
	return nil
}
