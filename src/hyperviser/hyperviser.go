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
	tNum      int
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

// leaderFields used by leader to maintain information about the state of the
// test it's leading.
type leaderFields struct {
	mu sync.Mutex
	readyWg *sync.WaitGroup
	doneWg *sync.WaitGroup
	servers *map[net.IP]serverInfo
	lg *log.Logger
	lgBuf io.WriteCloser
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
	lf *leaderFields
}

type rpcEndpoint string

const (
	status      rpcEndpoint = "Hyperviser.Status"
	addChord    rpcEndpoint = "Hyperviser.AddChord"
	removeChord rpcEndpoint = "Hyperviser.RemoveChord"
	prepareTest rpcEndpoint = "Hyperviser.PrepareTest"
	startTest   rpcEndpoint = "Hyperviser.StartTest"
	abortTest   rpcEndpoint = "Hyperviser.AbortTest"
	ready       rpcEndpoint = "Hyperviser.Ready"
	kill        rpcEndpoint = "Hyperviser.Kill"
	done        rpcEndpoint = "Hyperviser.Done"
	failed      rpcEndpoint = "Hyperviser.Failed"
)

// Call the given RPC function.
func callRPC(fname rpcEndpoint, ip string, pport int, timeout time.Duration,
	args, reply *interface{}) error {
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
	if _, ok := tests[args.TType]; !ok {
		return fmt.Errorf("Invalid test type: %s", args.TType)
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
	hv.ti.TNum++

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

	hv.chs = appEnd(hv.chs, chordSet{ch, kvs, rpcs})
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
		err := callRPC(ready, hv.ti.leader, defaultPort, rpcTimeout, args, reply)
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
		err = callRPC(failed, ti.leader.String(), defaultPort, rpcTimeout, args, &reply)
	} else {
		args := hv.makeAuthArgs()
		err = callRPC(done, ti.leader.String(), defaultPort, rpcTimeout, args, &reply)
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

	go hv.executeTest(hv.ti.testType, ti.tNum)
	return nil
}

// AbortTest allows the test leader to stop the test if something goes wrong.
func (hv *Hyperviser) AbortTest(args *AuthArgs, reply *struct{}) error {
	hv.mu.Lock()
	defer hv.mu.Unlock()
	if err := hv.validate(args); err != nil {
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

// RingModArgs used to change number of chord instances on a Hyperviser.
type RingModArgs struct {
	AA *AuthArgs
	N  int
}

// AddOne called to have hv start a new chord instance.
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
	var baseCh *chordkv.Chord
	if len(hv.chs) > 0 {
		baseCh = hv.chs[0].ch
	} else {
		baseCh = hv.ti.baseCh
	}

	for i := 0; i < args.N; i++ {
		if err := hv.addChord(baseCh); err != nil {
			return err
		}
	}

	return nil
}

// RemoveOne called to have hv remove a chord instance.
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
	if hv.chs == nil || len(hv.chs) == 0 {
		return fmt.Errorf("Cannot remove more chords")
	}

	// TODO: Better way to write this
	for i := 0; i < args.N; i++ {
		j := len(hv.chs) - 1
		if j < 0 {
			return fmt.Errorf("Cannot remove more chords: %d", i)
		}

		if err := hv.chs[j].clseChord(); err != nil {
			// TODO: if this happens, not clear how to handle state.
			return fmt.Errof("CloseChord failed: %s", err)
		}

		hv.chs = hv.chs[:j]
	}

	return nil
}

// HyperviserStatus contains basic information about the state of a Hyperviser.
type HyperviserStatus struct {
	IsRunning    bool
	IsTestConfig bool
	IsReady      bool
	IsTesting    bool
	IsLeader     bool
	Leader       net.IP
	TType        TestType
}

// Status returns the status of this hyperviser.
func (hv *Hyperviser) Status(args *AuthArgs, reply *HyperviserStatus) error {
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
	uninitialized serverStatus = "uninitialized"
	unresponsive  serverStatus = "unresponsive"
	failed        serverStatus = "failed"
	ready         serverStatus = "ready"
	preparing     serverStatus = "preparing"
	testing       serverStatus = "testing"
	done          serverStatus = "done"
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
func (hv *Hyperviser) initServers(targetNodes int) *map[net.IP]serverInfo {
	servers := make(map[net.IP]serverInfo)

	nodesPerServer := len(serverIPs) / batchSize
	nodesRemaining := len(serverIPs) % batchSize

	servers[hv.ip] = serverInfo{
		status: uninitialized,
		numChs: 0,
		targetNumChs: nodesPerServer,
		readyChan: make(chan bool),
		doneChan: make(chan bool),
		errChan: make(chan error)}

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

		servers[net.ParseIP(ip)] = serverInfo{
			status: uninitialized,
			numChs: 0,
			targetNumChs: nodesPerServer,
			readyChan: make(chan bool),
			doneChan: make(chan bool),
			errChan: make(chan error)}
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

	return servers
}

// cleanLeader resets state of leader after test execution completes. If
// useParentLock is true, acquire hv.mu.Lock before running. Otherwise, assume
// that lock has already been acquired.
func (hv *Hyperviser) cleanLeader(useParentLock bool) {
	if useParentLock {
		hv.mu.Lock()
		defer hv.mu.Unlock()
	}

	if hv.lf == nil {
		return
	}

	hv.lf.mu.Lock()
	defer hv.lf.mu.Unlock()

	hv.lf.readyWg = nil
	hv.lf.doneWg = nil

	for ip, info := range hv.lf.servers {
		if info.status != testing {
			continue
		}

		go func(ipp net.IP) {
			args := hv.makeAuthArgs()
			var reply struct{}
			err := callRPC(abortTest, ipp.String(), defaultPort, rpcTimeout, args, &reply)
			if err != nil {
				DPrintf("hv (%s): cleanLeader: abort %s failed: %s",
					hv.ip.String(), ipp.String(), err)
			}
		}(ip)
	}
	hv.lf.servers = nil

	if hv.lf.lg != nil {
		hv.lf.lgBuf.Close()
		hv.lf.lg = nil
		hv.lf.lgBuf = nil
	}
}

// sendLeaderReady used to send Prepare message in its own thread.
func (hv *Hyperviser) sendPrepare(ip net.IP, info *serverInfo, logName string) {
	args := &TestArgs{
		AA: hv.makeAuthArgs(),
		TType: hv.ti.testType,
		LogName: logName,
		NumChords: info.targetNumChs,
		BaseChord: hv.ti.baseCh}
	var reply struct{}

	timeout := time.Duration(info.targetNumChs) * time.Second + 5
	err := callRPC(prepareTest, ip.String(), defaultPort, timeout, args, &reply)

	hv.mu.Lock()
	hv.lf.mu.Lock()
	if err != nil {
		hv.lf.lg.Printf("sendPrepare: RPC to %s failed: %s",
			hv.ip.String(), ip.String(), err)
		info.status = unresponsive
	} else {
		info.status = preparing
	}
	hv.lf.mu.Unlock()
	hv.mu.Unlock()
}

// prepareLeader used to initialize leader in goroutine.
func (hv *Hyperviser) prepareLeader() {
	hv.mu.Lock()
	defer hv.mu.Unlock()

	for i := 1; i < hv.lf.servers[hv.ip].targetNumChs; i++ {
		if err := hv.addChord(hv.ti.baseCh); err != nil {
			hv.lf.lg.Printf("prepareLeader: initChord failed: %s", err)
			return
		}
	}

	hv.lf.readyWg.Done()
}

// StartLeader runs the given test as the leader.
func (hv *Hyperviser) StartLeader(testType TestType, leaderLog, testLog string) error {
	hv.mu.Lock()
	if !hv.isrunning {
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

	hv.lf = &leaderFields{}
	defer hv.cleanLeader(true)

	// Set up log files.
	f, err := os.Create(hv.logDir + leaderLog)
	if err != nil {
		return fmt.Errorf("Creating leader log failed: %s", err)
	}
	hv.lf.lgBuf = f
	hv.lf.lg = log.New(f, hv.ip.String(), log.LstdFlags|log.LUTC)

	f, err = os.Create(hv.logDir + testLog)
	if err != nil {
		return fmt.Errorf("Creating test log failed: %s", err)
	}
	hv.ti.lgBuf = f
	hv.ti.lg = log.New(f, hv.ip.String(), log.LstdFlags|log.LUTC)

	// Run the test
	// TODO: new log names for each batch
	for batchNum, batchSize := range tests[testType].phases {
		hv.lf.lg.Printf("Starting %s round %d", testType, batchNum)
		hv.ti.servers = initServers(batchSize)

		// Initialize leader's chords
		if err := hv.addChord(nil); err != nil {
			return fmt.Errorf("First chord initialization failed: %s", err)
		}
		hv.ti.baseCh = hv.chs[0].ch
		servers[hv.ip].numChs = 1

		// Prepare test on followers and leader
		hv.fl.lg.Println("Preparing for test")
		hv.lf.readyWg = &sync.WaitGroup{}
		for ip, info := range hv.ti.servers {
			hv.lf.readyWg.Add(1)
			if ip.Equal(hv.ip) {
				continue
			}
			go hv.sendPrepare(ip, info, testLog)
		}
		go hv.prepareLeader()

		// Gather responses
		hv.lf.lg.Println("Waiting for ready")
		readyChan := make(chan bool)
		go func() {
			hv.readyWg.Wait()
			readyChan <- true
		}()

		select {
		case <-time.After(readyTimeout):
			hv.lf.lg.Println("Timeout before servers ready")
			return
		case <-readyChan:
			// Nothing to do but move on.
		}

		// Run test
		hv.lf.lg.Println("Executing test")

		// TODO: here
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

	hv.lf.mu.Lock()
	defer hv.lf.mu.Unlock()

	if _, ok := hv.lf.servers[args.CallerIP]; !ok {
		return fmt.Errorf("Not part of test")
	}
	if hv.lf.servers[args.CallerIP].status != preparing {
		return fmt.Errorf("Called at wrong time")
	}

	hv.lf.servers[args.CallerIP].status = ready
	hv.lf.doneWg.Done()
	return nil
}

// Done called by test followers when they are done running their test.
func (hv *Hyperviser) Done(args *AuthArgs, reply *struct{}) error {
	// TODO: here
	return nil
}

// FailArgs contains reason why test failed.
type FailArgs struct {
	AA     *AuthArgs
	Reason error
}

// Failed called by test followers if their test failed for some reason.
func (hv *Hyperviser) Failed(args *FailArgs, reply *struct{}) error {
	// TODO: here
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

	if hv.ti.isLeader {
		hv.cleanLeader(false)
	}

	if hv.ti.isTesting {
		hv.stopTest(false)
	}

	hv.servListener.Close()
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

type testInfo struct {
	phases []int
	f      func(*hyperviser.Hyperviser) error
}

var tests = map[TestType]testInfo{
	LookupPerf: testInfo{
		phases: []int{10, 30, 60, 90, 120, 150, 180, 200},
		f:      lookupPerf},
	HelloWorld: testInfo{
		phases: []int{1, 20},
		f:      helloWorld},
}

// lookupPerf measures lookup latency as a function of number of nodes in the
// chord ring.
func lookupPerf(hv *Hyperviser) error {

}

// helloWorld used to test the hyperviser framework. Plz work
func helloWorld(hv *Hyperviser) error {
	hv.ti.lg.Println("Hello World")
	return nil
}
