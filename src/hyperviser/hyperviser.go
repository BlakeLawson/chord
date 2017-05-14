// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package hyperviser

import (
	"chordkv"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"port"
	"strings"
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

// AddrPair stores IP and Port.
type AddrPair struct {
	IP   string
	Port int
}

// String returns string representation of ap.
func (ap *AddrPair) String() string {
	return fmt.Sprintf("%s:%d", ap.IP, ap.Port)
}

const minPort = 1024
const maxPort = 1<<16 - 1

// Validate ensures AddrPair contains a valid ip address and port.
func (ap *AddrPair) Validate() error {
	ip := net.ParseIP(ap.IP)
	if ip == nil {
		return fmt.Errorf("Invalid IP address")
	}

	if ap.Port <= minPort || ap.Port >= maxPort {
		return fmt.Errorf("Invalid port number")
	}

	return nil
}

// Store known server IP addresses. Used by test leader to coordinate.
var serverAddrs = map[AddrPair]bool{
	AddrPair{"54.191.191.206", defaultPort}: true,
	AddrPair{"54.187.20.229", defaultPort}:  true,
}

type testInfo struct {
	tNum      int
	tSize     int
	isTesting bool
	isRunning bool
	isReady   bool
	readyChan chan bool
	killChan  chan bool
	isLeader  bool
	leader    AddrPair
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
	servers *map[AddrPair]*serverInfo
	lg      *log.Logger
	lgBuf   io.WriteCloser
}

// Hyperviser manages...
type Hyperviser struct {
	isRunning    bool
	pass         string
	logDir       string
	mu           sync.Mutex
	ap           AddrPair
	servListener net.Listener
	ti           testInfo
	chkvs        *[]*chordkv.ChordKV
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
func callRPC(fname rpcEndpoint, ap AddrPair, timeout time.Duration,
	args, reply interface{}) error {
	// TODO: Use TLS
	conn, err := net.DialTimeout("tcp", ap.String(), rpcTimeout)
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
	DPrintf("in ti.clear()")
	ti.isTesting = false
	ti.isRunning = false
	ti.isReady = false
	ti.isLeader = false
	ti.leader = AddrPair{"", 0}
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
	AP       AddrPair
	Password string
}

func (hv *Hyperviser) makeAuthArgs() *AuthArgs {
	return &AuthArgs{hv.ap, hv.pass}
}

// Write value to bool channel.
func writeBool(c chan bool) {
	c <- true
}

// validate the IP address given to an RPC endpoint. THIS METHOD ASSUMES THAT
// IT IS CALLED FROM A LOCKING CONTEXT.
func (hv *Hyperviser) validate(aa *AuthArgs) error {
	if !hv.isRunning {
		return fmt.Errorf("Not running", hv.ap.IP)
	}

	if err := aa.AP.Validate(); err != nil {
		return fmt.Errorf("Invalid AddrPair")
	}
	if _, ok := serverAddrs[aa.AP]; !ok {
		return fmt.Errorf("Invalid IP address")
	}
	if aa.Password != hv.pass {
		return fmt.Errorf("Invalid password")
	}
	if hv.ti.isTesting && !hv.ti.isLeader && hv.ti.leader != aa.AP {
		return fmt.Errorf("Not called by leader")
	}
	if hv.ti.isTesting && hv.ti.isLeader {
		hv.ls.mu.Lock()
		defer hv.ls.mu.Unlock()
		if _, ok := (*hv.ls.servers)[aa.AP]; !ok {
			return fmt.Errorf("Not part of test")
		}
	}

	return nil
}

// Shut down all of the chord instances associated with the current test. THiS
// METHOD ASSUMES THAT IT IS CALLED FROM A LOCKING CONTEXT.
func (hv *Hyperviser) closeChords() {
	DPrintf("hv (%s): closeChords", hv.ap.String())
	if hv.chkvs == nil {
		return
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < len(*hv.chkvs); i++ {
		if (*hv.chkvs)[i] == nil {
			continue
		}

		wg.Add(1)
		go func(chkv *chordkv.ChordKV) {
			defer wg.Done()
			if err := chkv.Kill(); err != nil {
				DPrintf("hv (%s): error closing chord: %s", hv.ap.String(), err)
			}
		}((*hv.chkvs)[i])
	}

	wg.Wait()
	hv.chkvs = nil
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
	DPrintf("hv (%s): PrepareTest %s called from %s", hv.ap.String(),
		args.TType, args.AA.AP.String())
	hv.mu.Lock()
	defer hv.mu.Unlock()
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
	hv.ti.tSize = args.NumChords
	hv.ti.leader = args.AA.AP
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
	hv.ti.lg = log.New(f, hv.ap.String()+" ", log.LstdFlags|log.LUTC)
	hv.ti.tNum++

	// Prepare the test.
	go hv.initTest(args.NumChords)

	return nil
}

// Stop the current running test and reset state. useLocks parameter used to
// indicate whether stopTest needs to acquire locks. If useLocks set to false
// this method assumes that the calling function acquired appropriate locks.
func (hv *Hyperviser) stopTest(useLocks bool) {
	DPrintf("hv (%s): stopTest", hv.ap.String())
	if useLocks {
		hv.mu.Lock()
		defer hv.mu.Unlock()
	}

	DPrintf("hv (%s): stopTest: calling closeChords", hv.ap.String())
	hv.closeChords()
	DPrintf("hv (%s): stopTest: calling clear", hv.ap.String())
	hv.ti.clear()
}

// Return a randomly chosen port. Alternative to port.New().
func pickRandomPort() (int, error) {
	var p int
	done := false
	for i := 0; i < 50; i++ {
		p = rand.Intn(maxPort-minPort) + minPort
		if done, _ = port.Check(p); done {
			break
		}
	}

	if done {
		return p, nil
	}
	return 0, fmt.Errorf("Couldn't find port")
}

// Add a chord instance to hv using baseCh to initialize the new chord. THIS
// METHOD ASSUMES THAT IT IS CALLED FROM A LOCKING CONTEXT.
func (hv *Hyperviser) addChord(baseChNode *chordkv.Node) error {
	// Try to add chord up to three times. addChordBase usually failed because
	// it tried to make a chord instance with a port that is already in use, so
	// trying again should normally fix the problem.
	var err error
	for i := 0; i < 3; i++ {
		if i > 0 {
			DPrintf("hv (%s): addChord: retrying", hv.ap.String())
		}
		err = hv.addChordBase(baseChNode)
		if err == nil {
			return nil
		}
		DPrintf("hv (%s): addChord failed%d: %s", hv.ap.String(), i+1, err)
	}
	return err
}

// Add a chord instance to hv using baseCh to initialize the new chord. THIS
// METHOD ASSUMES THAT IT IS CALLED FROM A LOCKING CONTEXT.
func (hv *Hyperviser) addChordBase(baseChNode *chordkv.Node) error {
	if baseChNode != nil {
		DPrintf("hv (%s): addChordBase: baseChNode = %s(%016x)", hv.ap.String(),
			baseChNode.String(), baseChNode.Hash)
	} else {
		DPrintf("hv (%s): addChordBase: baseChNode = nil", hv.ap.String())
	}

	// Ensure everything is initialized.
	if hv.chkvs == nil {
		hv.chkvs = new([]*chordkv.ChordKV)
	}

	DPrintf("hv (%s): addChordBase: calling chordkv.MakeChordKV", hv.ap.String())
	chkv, err := chordkv.MakeChordKV(baseChNode)
	if err != nil {
		return err
	}
	*hv.chkvs = append(*hv.chkvs, chkv)

	if debug {
		n := chkv.Ch.GetNode()
		CPrintf(Yellow, "hv (%s): addChordBase: created chord %s [%016x]",
			hv.ap.String(), n.String(), n.Hash)
	}

	time.Sleep(time.Second)
	return nil
}

// initTest configures this hyperviser instance to test.
func (hv *Hyperviser) initTest(numChords int) {
	hv.mu.Lock()
	defer hv.mu.Unlock()
	DPrintf("hv (%s): initTest(%d)", hv.ap.String(), numChords)
	if numChords <= 0 {
		DPrintf("hv (%s): initTest: invalid numChords (%d)", hv.ap.String(), numChords)
		hv.ti.clear()
		return
	}
	if hv.chkvs != nil {
		DPrintf("hv (%s): initTest: chord instances already exist", hv.ap.String())
	}

	CPrintf(Blue, "hv (%s): initTest: after validation", hv.ap.String())
	for i := 0; i < numChords; i++ {
		if err := hv.addChord(hv.ti.baseCh); err != nil {
			DPrintf("hv (%s): initTest: addChord failed: %s", hv.ap.String(), err)
			hv.stopTest(false)
			return
		}
	}

	hv.ti.isReady = true
	hv.ti.readyChan = make(chan bool)

	// Tell leader hv is ready.
	go func() {
		args := hv.makeAuthArgs()
		var reply struct{}
		DPrintf("hv (%s): initTest: calling ready", hv.ap.String())
		err := callRPC(readyRPC, hv.ti.leader, rpcTimeout, args, &reply)
		if err != nil {
			DPrintf("hv (%s): Ready failed: %s", hv.ap.String(), err)
			if strings.Contains(err.Error(), "body gob") {
				DPrintf("hv (%s): More info: leader:%s args: %+v",
					hv.ap.String(), hv.ti.leader.String(), args)
			}
		}
	}()

	// Goroutine to start leader response timeout
	go func() {
		select {
		case <-time.After(readyTimeout):
			// Clean up everything.
			DPrintf("hv (%s): initTest: clean up from readyTimeout", hv.ap.String())
			hv.stopTest(true)
		case <-hv.ti.killChan:
			hv.stopTest(true)
		case <-hv.ti.readyChan:
			// Nothing to do but exit.
		}
	}()
}

func (hv *Hyperviser) executeTest(tt TestType, tNum int) {
	DPrintf("hv (%s): executeTest %s", hv.ap.String(), tt)
	err := tests[tt].f(hv)
	hv.mu.Lock()
	if hv.ti.tNum != tNum {
		// Test changed somehow.
		DPrintf("hv (%s): test changed from %d -> %d during test %d",
			hv.ap.String(), tNum, hv.ti.tNum, tNum)
		hv.mu.Unlock()
		return
	}

	ti := hv.ti
	hv.mu.Unlock()

	var reply struct{}
	if err != nil {
		args := FailArgs{hv.makeAuthArgs(), err}
		err = callRPC(failedRPC, ti.leader, rpcTimeout, args, &reply)
		DPrintf("hv (%s): executeTest failed: %s", hv.ap.String(), err)
	} else {
		args := hv.makeAuthArgs()
		err = callRPC(doneRPC, ti.leader, rpcTimeout, args, &reply)
	}

	if err != nil {
		hv.ti.lg.Printf("hv (%s): executeTest: RPC failed: %s", hv.ap.String(), err)
	}

	DPrintf("hv (%s): executeTest: calling stopTest", hv.ap.String())
	hv.stopTest(true)
}

// StartTest serves as a synchronization barrier for tests so that the test
// leader can ensure all hypervisers are ready.
func (hv *Hyperviser) StartTest(args *TestArgs, reply *struct{}) error {
	hv.mu.Lock()
	defer hv.mu.Unlock()
	DPrintf("hv (%s): StartTest from %s", hv.ap.String(), args.AA.AP.String())
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
	DPrintf("hv (%s): AbortTest from %s", hv.ap.String(), args.AP.String())
	hv.mu.Lock()
	defer hv.mu.Unlock()
	DPrintf("hv (%s): AbortTest from %s", hv.ap.String(), args.AP.String())
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
	DPrintf("hv (%s): AbortTest: calling stopTest", hv.ap.String())
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
	if len(*hv.chkvs) > 0 {
		n := (*hv.chkvs)[0].Ch.GetNode()
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
	if hv.chkvs == nil || len(*hv.chkvs) == 0 {
		return fmt.Errorf("Cannot remove more chords")
	}

	// TODO: Better way to write this
	for i := 0; i < args.N; i++ {
		j := len(*hv.chkvs) - 1
		if j < 0 {
			return fmt.Errorf("Cannot remove more chords: %d", i)
		}

		if err := (*hv.chkvs)[j].Kill(); err != nil {
			// TODO: if this happens, unclear how to handle state.
			return fmt.Errorf("CloseChord failed: %s", err)
		}

		*hv.chkvs = (*hv.chkvs)[:j]
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
	Leader       AddrPair
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
}

// initServers returns set of servers to use for test. Determines how many
// chord instances each server should run. Guarantees that hv is part of the
// set of servers.
func (hv *Hyperviser) initServers(targetNodes int) *map[AddrPair]*serverInfo {
	servers := make(map[AddrPair]*serverInfo)

	nodesPerServer := targetNodes / len(serverAddrs)
	nodesRemaining := targetNodes % len(serverAddrs)

	servers[hv.ap] = &serverInfo{
		status:       uninitializedSt,
		numChs:       0,
		targetNumChs: nodesPerServer}

	var count int
	if nodesPerServer == 0 {
		count = nodesRemaining - 1
	} else {
		count = len(serverAddrs) - 1
	}

	// Initialize serverInfos
	for ap := range serverAddrs {
		if count == 0 {
			break
		}
		if ap == hv.ap {
			continue
		}

		servers[ap] = &serverInfo{
			status:       uninitializedSt,
			numChs:       0,
			targetNumChs: nodesPerServer}
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
	DPrintf("hv (%s): cleanLeader", hv.ap.String())
	if useParentLock {
		DPrintf("hv (%s): cleanLeader about to lock", hv.ap.String())
		hv.mu.Lock()
		defer hv.mu.Unlock()
	}

	if hv.ls == nil {
		return
	}

	hv.ls.mu.Lock()
	defer hv.ls.mu.Unlock()

	for ap, info := range *hv.ls.servers {
		if info.status != testingSt {
			continue
		}

		go func(app AddrPair) {
			args := hv.makeAuthArgs()
			var reply struct{}
			err := callRPC(abortTestRPC, app, rpcTimeout, args, &reply)
			if err != nil {
				DPrintf("hv (%s): cleanLeader: abort %s failed: %s",
					hv.ap.String(), app.String(), err)
			}
		}(ap)
	}
	hv.ls.servers = nil

	if hv.ls.lg != nil {
		hv.ls.lgBuf.Close()
		hv.ls.lg = nil
		hv.ls.lgBuf = nil
	}
}

// sendLeaderReady used to send Prepare message in its own thread.
func (hv *Hyperviser) sendPrepare(ap AddrPair, info *serverInfo, logName string) {
	args := &TestArgs{
		AA:            hv.makeAuthArgs(),
		TType:         hv.ti.testType,
		LogName:       logName,
		NumChords:     info.targetNumChs,
		BaseChordNode: hv.ti.baseCh}
	var reply struct{}

	timeout := time.Duration(info.targetNumChs)*time.Second + 5
	err := callRPC(prepareTestRPC, ap, timeout, args, &reply)
	if err != nil {
		DPrintf("hv (%s): sendPrepare: RPC to %s failed: %s",
			hv.ap.String(), ap.String(), err)
		hv.ls.lg.Printf("sendPrepare: RPC to %s failed: %s",
			hv.ap.String(), ap.String(), err)
	}

	hv.mu.Lock()
	hv.ls.mu.Lock()
	// This if statement seems redundant but it uses the lock for less time than
	// if everything was done using a single if statement.
	if err != nil {
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
	DPrintf("hv (%s): prepareLeader", hv.ap.String())
	info := (*hv.ls.servers)[hv.ap]
	for i := 1; i < info.targetNumChs; i++ {
		if err := hv.addChord(hv.ti.baseCh); err != nil {
			hv.ls.lg.Printf("prepareLeader: initChord failed: %s", err)
			return
		}
	}

	hv.ti.tNum = info.targetNumChs
	info.numChs = info.targetNumChs
	info.status = readySt
	CPrintf(Red, "hv (%s): prepareLeader Done", hv.ap.String())
	hv.ls.readyWg.Done()
}

// sendStart used to call start test in other thread.
func (hv *Hyperviser) sendStart(ap AddrPair, info *serverInfo, logName string) {
	DPrintf("hv (%s): sendStart", hv.ap.String())
	info.status = testingSt
	args := TestArgs{
		AA:            hv.makeAuthArgs(),
		TType:         hv.ti.testType,
		LogName:       logName,
		NumChords:     info.targetNumChs,
		BaseChordNode: hv.ti.baseCh}
	var reply struct{}
	err := callRPC(startTestRPC, ap, rpcTimeout, args, &reply)

	hv.mu.Lock()
	hv.ls.mu.Lock()
	if err != nil {
		DPrintf("hv (%s): sendStart: RPC to %s failed: %s",
			hv.ap.String(), ap.String(), err)
		hv.ls.lg.Printf("sendStart: RPC to %s failed: %s", ap.String(), err)
		info.status = unresponsiveSt
	}

	DPrintf("hv (%s): sendStart: releasing locks", hv.ap.String())
	hv.ls.mu.Unlock()
	hv.mu.Unlock()
}

// startLeaderTest runs current test on leader.
func (hv *Hyperviser) startLeaderTest() {
	DPrintf("hv (%s): startLeaderTest", hv.ap.String())
	err := tests[hv.ti.testType].f(hv)

	hv.ls.mu.Lock()
	info := (*hv.ls.servers)[hv.ap]
	if err != nil {
		hv.ls.lg.Printf("Node %s test failed: %s", hv.ap.String(), err)
		info.status = failedSt
	} else {
		info.status = doneSt
	}

	DPrintf("hv (%s): startLeaderTest: calling doneWg.Done()", hv.ap.String())
	hv.ls.doneWg.Done()
	hv.ls.mu.Unlock()
	DPrintf("hv (%s): startLeaderTest: exiting", hv.ap.String())
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
	defer func() {
		DPrintf("hv (%s): StartLeader: calling stopTest on exit", hv.ap.String())
		hv.stopTest(true)
	}()

	hv.ls = &leaderState{}
	defer func() {
		DPrintf("hv (%s): StartLeader: calling cleanLeader on exit", hv.ap.String())
		hv.cleanLeader(true)
	}()

	// Set up log files.
	f, err := os.Create(hv.logDir + leaderLog)
	if err != nil {
		return fmt.Errorf("Creating leader log failed: %s", err)
	}
	hv.ls.lgBuf = f
	hv.ls.lg = log.New(f, hv.ap.String()+" ", log.LstdFlags|log.LUTC)

	// Run the test
	// TODO: should rewrite to reuse existing chord instances rather than
	// tearing down every time.
	for batchNum, batchSize := range tests[testType].phases {
		logName := fmt.Sprintf("%s%d", testLog, batchNum)
		f, err = os.Create(hv.logDir + logName)
		if err != nil {
			return fmt.Errorf("Creating test log failed: %s", err)
		}
		hv.ti.lgBuf = f
		hv.ti.lg = log.New(f, hv.ap.String()+" ", log.LstdFlags|log.LUTC)

		hv.ls.lg.Printf("Starting %s round %d", testType, batchNum)
		CPrintf(White, "hv (%s): Starting %s round %d", hv.ap.String(), testType, batchNum)
		hv.ls.servers = hv.initServers(batchSize)

		// Initialize leader's chords
		if err := hv.addChord(nil); err != nil {
			return fmt.Errorf("First chord initialization failed: %s", err)
		}

		n := (*hv.chkvs)[0].Ch.GetNode()
		hv.ti.baseCh = &n
		CPrintf(White, "hv (%s): baseChord: %s", hv.ap.String(), hv.ti.baseCh.String())

		info := (*hv.ls.servers)[hv.ap]
		info.numChs = 1

		// Prepare test on followers and leader
		hv.ls.lg.Println("Preparing for test")
		DPrintf("hv (%s): Preparing for test", hv.ap.String())
		hv.ls.readyWg = &sync.WaitGroup{}

		hv.mu.Lock()
		hv.ti.isReady = true
		hv.mu.Unlock()
		for ap, info := range *hv.ls.servers {
			hv.ls.readyWg.Add(1)
			if ap == hv.ap {
				continue
			}
			go hv.sendPrepare(ap, info, logName)
		}
		go hv.prepareLeader()

		// Gather responses
		hv.ls.lg.Println("Waiting for ready")
		DPrintf("hv (%s): Waiting for ready", hv.ap.String())

		waitChan := make(chan bool)
		go func() {
			hv.ls.readyWg.Wait()
			waitChan <- true
		}()
		timeout := 10*time.Second + time.Second*time.Duration(info.targetNumChs)
		DPrintf("timeout: %d", timeout/time.Second)
		select {
		case <-time.After(timeout):
			msg := "Timeout before servers ready"
			hv.ls.lg.Println(msg)
			return fmt.Errorf(msg)
		case <-waitChan:
			// Nothing to do but move on.
		}

		// Run test
		hv.mu.Lock()
		hv.ti.isRunning = true
		hv.ls.doneWg = &sync.WaitGroup{}
		hv.mu.Unlock()
		hv.ls.lg.Println("Executing test")
		DPrintf("hv (%s): Executing test", hv.ap.String())
		for ap, info := range *hv.ls.servers {
			DPrintf("in servers loop: ap=%s info=%+v", ap.String(), *info)
			hv.ls.doneWg.Add(1)
			if ap == hv.ap {
				DPrintf("skipping loop iteration")
				continue
			}
			go hv.sendStart(ap, info, logName)
		}
		go hv.startLeaderTest()

		hv.ls.lg.Println("Waiting for test to finish")
		DPrintf("hv (%s): Waiting for test to finish", hv.ap.String())
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
		DPrintf("hv (%s): Test %d Succeeded", hv.ap.String(), batchNum)

		// Clean up leader
		hv.mu.Lock()
		DPrintf("hv (%s): StartLeader: calling choseChords after successful test",
			hv.ap.String())
		hv.closeChords()
		hv.mu.Unlock()
	}

	return nil
}

// Ready called by test followers when they are ready to begin the test.
func (hv *Hyperviser) Ready(args *AuthArgs, reply *struct{}) error {
	CPrintf(Blue, "hv (%s): Ready from %s", hv.ap.String(), args.AP.String())

	hv.mu.Lock()
	if err := hv.validate(args); err != nil {
		hv.mu.Unlock()
		return err
	}
	if !hv.ti.isLeader {
		hv.mu.Unlock()
		return fmt.Errorf("Not leader")
	}
	if !hv.ti.isReady {
		hv.mu.Unlock()
		return fmt.Errorf("Not ready")
	}
	hv.mu.Unlock()

	hv.ls.mu.Lock()
	defer hv.ls.mu.Unlock()

	info := (*hv.ls.servers)[args.AP]
	if info.status != preparingSt {
		return fmt.Errorf("Called at wrong time: %s", info.status)
	}

	info.status = readySt
	hv.ls.readyWg.Done()
	DPrintf("hv (%s): Ready (%s): return success", hv.ap.String(), args.AP.String())
	return nil
}

// Done called by test followers when they are done running their test.
func (hv *Hyperviser) Done(args *AuthArgs, reply *struct{}) error {
	DPrintf("hv (%s): Done (%s)", hv.ap.String(), args.AP.String())
	hv.mu.Lock()
	DPrintf("hv (%s): Done (%s): locks1 acquired", hv.ap.String(), args.AP.String())
	if err := hv.validate(args); err != nil {
		hv.mu.Unlock()
		return err
	}
	if !hv.ti.isLeader {
		hv.mu.Unlock()
		return fmt.Errorf("Not leader")
	}
	if !hv.ti.isReady {
		hv.mu.Unlock()
		return fmt.Errorf("Not ready")
	}
	if !hv.ti.isRunning {
		hv.mu.Unlock()
		return fmt.Errorf("Not running test")
	}
	hv.mu.Unlock()

	DPrintf("hv (%s): Done (%s): getting second locks", hv.ap.String(),
		args.AP.String())
	hv.ls.mu.Lock()
	defer hv.ls.mu.Unlock()
	DPrintf("hv (%s): Done (%s): locks2 acquired", hv.ap.String(),
		args.AP.String())

	info := (*hv.ls.servers)[args.AP]
	if info.status != testingSt {
		DPrintf("hv (%s): Done (%s): info.status (%s) != testingSt (%s)",
			hv.ap.String(), args.AP.String(), info.status, testingSt)
		return fmt.Errorf("Called at wrong time: current status is %s", info.status)
	}

	info.status = doneSt
	DPrintf("hv (%s): Done: updating doneWg", hv.ap.String())
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
	DPrintf("hv (%s): Failed from %s: %s", hv.ap.String(), args.AA.AP.String(),
		args.Reason)
	hv.mu.Lock()
	if err := hv.validate(args.AA); err != nil {
		hv.mu.Unlock()
		return err
	}
	if !hv.ti.isLeader {
		hv.mu.Unlock()
		return fmt.Errorf("Not leader")
	}
	if !hv.ti.isReady {
		hv.mu.Unlock()
		return fmt.Errorf("Not ready")
	}
	if !hv.ti.isRunning {
		hv.mu.Unlock()
		return fmt.Errorf("Not running test")
	}
	hv.mu.Unlock()

	hv.ls.mu.Lock()
	defer hv.ls.mu.Unlock()

	info := (*hv.ls.servers)[args.AA.AP]
	if info.status != testingSt {
		return fmt.Errorf("Called at wrong time: %s", info.status)
	}

	hv.ls.lg.Printf("Node %s test failed: %s",
		args.AA.AP.String(), args.Reason)
	info.status = failedSt
	DPrintf("hv (%s): Failed: updating doneWg", hv.ap.String())
	hv.ls.doneWg.Done()
	return nil
}

// Make a new Hyperviser.
func Make(ip, pass, logDir string) (*Hyperviser, error) {
	p1, _ := port.New()
	serverAddrs = map[AddrPair]bool{
		AddrPair{"127.0.0.1", p1}: true,
	}
	return makePort(ip, pass, logDir, defaultPort)
}

func makePort(ip, pass, logDir string, port int) (*Hyperviser, error) {
	hv := &Hyperviser{}
	hv.pass = pass
	hv.logDir = logDir // TODO: logDir validation
	hv.ap = AddrPair{ip, port}

	if err := hv.ap.Validate(); err != nil {
		return nil, err
	}

	// TODO: Add TLS
	var err error
	hv.servListener, err = net.Listen("tcp", hv.ap.String())
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
	DPrintf("hv (%s): Stop", hv.ap.String())
	if useLocks {
		DPrintf("hv (%s): Stop: getting lock", hv.ap.String())
		hv.mu.Lock()
		defer hv.mu.Unlock()
	}

	if !hv.isRunning {
		return
	}

	if hv.ti.isLeader {
		DPrintf("hv (%s): Stop: calling cleanLeader", hv.ap.String())
		hv.cleanLeader(false)
	}

	if hv.ti.isTesting {
		DPrintf("hv (%s): Stop: calling stopTest", hv.ap.String())
		hv.stopTest(false)
	}

	DPrintf("hv (%s): Stop: closing servListner", hv.ap.String())
	hv.servListener.Close()
	hv.isRunning = false
	DPrintf("hv (%s): Stop: exiting", hv.ap.String())
}

// Kill shuts down the Hyperviser.
func (hv *Hyperviser) Kill(args *AuthArgs, reply *struct{}) error {
	DPrintf("hv (%s): Kill from %s", hv.ap.String(), args.AP.String())
	hv.mu.Lock()
	defer hv.mu.Unlock()
	if err := hv.validate(args); err != nil {
		return err
	}

	DPrintf("hv (%s): Kill: calling Stop", hv.ap.String())
	hv.Stop(false)
	return nil
}

// TESTS ...

// TestType indicates the test that should be performed.
type TestType string

const (
	LookupPerf TestType = "LookupPerf"
	HelloWorld TestType = "HelloWorld"
	KeyDist    TestType = "KeyDist"
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
		timeout: 3 * time.Minute},
	HelloWorld: testConfig{
		phases:  []int{1, 2, 4, 7, 10, 20, 50},
		f:       helloWorld,
		timeout: 10 * time.Second},
	KeyDist: testConfig{
		phases:  []int{10, 30, 60, 90, 120, 150, 180, 200},
		f:       keyDist,
		timeout: 4 * time.Minute},
}

// Return random Chord key.
func generateKey() chordkv.UHash {
	r := rand.Uint32()
	return chordkv.Hash(fmt.Sprintf("%08x", r))
}

// lookupPerf measures lookup latency as a function of number of nodes in the
// chord ring. Writes data to log file in form:
//     Lookup <ChordID> <Lookup Key> <Succeeded|Failed> <num hops> <latency>
func lookupPerf(hv *Hyperviser) error {
	DPrintf("hv (%s): lookupPerf", hv.ap.String())

	// From Chord paper: each physical site issues 16 Chord lookups for randomly
	// chosen keys one-by-one.
	const numRequests = 16

	// Only sends one set of requests per physical site, so same node
	// for everything.
	ch := (*hv.chkvs)[0].Ch
	var outErr error
	for i := 0; i < numRequests; i++ {
		r := generateKey()
		_, info, err := ch.Lookup(r)
		if err != nil {
			outErr = err
			DPrintf("hv (%s): lookupPerf: lookup %016x on %016x failed: %s",
				hv.ap.String(), r, ch.GetID())
			hv.ti.lg.Printf("Lookup %016x -> %016x Failed %s", ch.GetID(), r, err)
		}

		hv.ti.lg.Printf("Lookup %016x -> %016x Succeeded %d %f",
			ch.GetID(), r, info.Hops, info.Latency.Seconds())
	}

	// Make sure all nodes have time to finish.
	time.Sleep(30 * time.Second)
	return outErr
}

// helloWorld used to test the hyperviser framework. Plz work
func helloWorld(hv *Hyperviser) error {
	DPrintf("hv (%s): helloWorld", hv.ap.String())
	hv.ti.lg.Println("Hello World")
	time.Sleep(2 * time.Second)
	return nil
}

// keyDist measures the distribution of keys in the chord network as a function
// of network size.
func keyDist(hv *Hyperviser) error {
	return nil
}
