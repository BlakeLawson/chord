// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package chordkv

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"net"
	"sort"
	"testing"
	"time"
)

// nodeSorter used to sort array of nodes by hash.
type nodeSorter struct {
	chords *[]*Chord
}

// Len is part of sort.Interface.
func (ns *nodeSorter) Len() int {
	return len(*ns.chords)
}

// Swap is part of sort.Interface.
func (ns *nodeSorter) Swap(i, j int) {
	(*ns.chords)[i], (*ns.chords)[j] = (*ns.chords)[j], (*ns.chords)[i]
}

// Less is part of sort.Interface.
func (ns *nodeSorter) Less(i, j int) bool {
	return (*ns.chords)[i].n.Hash < (*ns.chords)[j].n.Hash
}

// Given array of chord instances sorted by hash and a desired node hash, return
// the array index for the node responsible for the target hash.
func findKeyOwner(chords *[]*Chord, target UHash) int {
	size := len(*chords)
	if size <= 1 {
		return 0
	}

	for i := 0; i < size; i++ {
		if inRange(target, (*chords)[i].n.Hash, (*chords)[(i+1)%size].n.Hash) {
			return (i + 1) % size
		}
	}

	// This should never happen.
	return -1
}

func initializeChordRing(size int) error {
	if size <= 0 {
		return fmt.Errorf("initializeChordRing called with invalid size %d", size)
	}

	var err error
	localhost := net.ParseIP("127.0.0.1")
	basePort := 8888
	sharedKV := &KVServer{}
	rpcInstances := make([]*RPCServer, size)
	chordInstances := make([]*Chord, size)

	// First node
	n := MakeNode(localhost, basePort)
	chordInstances[0], err = MakeChord(n, Node{}, true)
	if err != nil {
		return fmt.Errorf("Chord[0] initialization failed: %s", err)
	}

	rpcInstances[0], err = startRPC(chordInstances[0], sharedKV, n.String())
	if err != nil {
		chordInstances[0].Kill()
		return fmt.Errorf("RPCServer[0] initialization failed: %s", err)
	}

	// End chord instance before server.
	defer rpcInstances[0].End()
	defer chordInstances[0].Kill()

	DPrintf("Initialized chord[%s] (%016x)", chordInstances[0].n.String(),
		chordInstances[0].n.Hash)

	// Initialization phase.
	for i := 1; i < size; i++ {
		n = MakeNode(localhost, basePort+i)

		// Use dummyCh to initialize RPC server before calling MakeChord
		dummyCh := &Chord{}
		rpcInstances[i], err = startRPC(dummyCh, sharedKV, n.String())
		if err != nil {
			chordInstances[i].Kill()
			return fmt.Errorf("RPCServer[%d] initialization failed: %s", i, err)
		}
		defer rpcInstances[i].End()
		rpcInstances[i].initBarrier.Lock()

		chordInstances[i], err = MakeChord(n, chordInstances[0].n, false)
		if err != nil {
			rpcInstances[i].initBarrier.Unlock()
			return fmt.Errorf("Chord[%d] initiailzation failed: %s", i, err)
		}
		defer chordInstances[i].Kill()

		rpcInstances[i].ch = chordInstances[i]
		rpcInstances[i].initBarrier.Unlock()

		DPrintf("Initialized chord[%s] (%016x)", chordInstances[i].n.String(),
			chordInstances[i].n.Hash)

		// Give time to stabilize
		time.Sleep(2 * stabilizeTimeout)
	}

	// Validation phase.
	ns := &nodeSorter{&chordInstances}
	sort.Sort(ns)
	for i := 0; i < size; i++ {
		// Check successor pointers
		if chordInstances[i].ftable[0].Hash != chordInstances[(i+1)%size].n.Hash {
			return fmt.Errorf("Chord[%d] successor invalid", i)
		}

		// Check predecessor pointers
		idx := i - 1
		if i == 0 {
			idx = size - 1
		}
		if chordInstances[i].predecessor.Hash != chordInstances[idx].n.Hash {
			return fmt.Errorf("Chord[%d] predecessor invalid", i)
		}

		// Check finger table
		for j := 1; j < len(chordInstances[i].ftable); j++ {
			expectedIdx := findKeyOwner(&chordInstances, chordInstances[i].ftableStart(j))
			if chordInstances[i].ftable[j].Hash != chordInstances[expectedIdx].n.Hash {
				ch := chordInstances[i]
				chHash := ch.n.Hash
				DPrintf("Chord[%016x].ftable:", chHash)
				for k, n := range ch.ftable {
					DPrintf("ftable[%02d]: %016x", k, n.Hash)
				}
				CPrintf(White, "Chord[%016x] ftable entry %d incorrect", chHash, j)
				CPrintf(White, "Expected ch[%016x].ftable[%02d] = %016x", chHash, j, chordInstances[expectedIdx].n.Hash)
				CPrintf(White, "Actual   ch[%016x].ftable[%02d] = %016x", chHash, j, ch.ftable[j].Hash)
				chString := ""
				for _, ch := range chordInstances {
					chString += fmt.Sprintf("%016x ", ch.n.Hash)
				}
				DPrintf("Set of chords: [ %s]", chString)
				return fmt.Errorf("Chord[%d] ftable entry %d incorrect", i, j)
			}
		}

		// TODO: Add more invariant checks
	}

	return nil
}

func TestChordOneInitialization(t *testing.T) {
	fmt.Println("Test: Chord single initializtion ...")
	err := initializeChordRing(1)
	if err != nil {
		t.Fatalf("Chord single initialization failed: %s", err)
	}

	fmt.Println(" ... Passed")
}

func TestChordTwoInitializtion(t *testing.T) {
	fmt.Println("Test: Chord double initializaiton ...")
	err := initializeChordRing(2)
	if err != nil {
		t.Fatalf("Chord double initialization failed: %s", err)
	}

	fmt.Println(" ... Passed")
}

func TestChordThreeInitialization(t *testing.T) {
	fmt.Println("Test: Chord triple initialization ...")
	err := initializeChordRing(3)
	if err != nil {
		t.Fatalf("Chord triple initialization failed: %s", err)
	}

	fmt.Println(" ... Passed")
}

// This function may give an error that says something along the lines of "too
// many open files". There's a way to adjust the maximum number of files you
// can have open at once, but Blake hasn't tried it yet.
func TestChordManyInitialization(t *testing.T) {
	testSize := 50
	fmt.Printf("Test: Chord %d initializations ...\n", testSize)
	err := initializeChordRing(testSize)
	if err != nil {
		t.Fatalf("Chord many initialization failed: %s", err)
	}

	fmt.Println(" ... Passed")
}

// Create single chord instance with hard coded fields. Fills ftable and slist
// with fake nodes at ideal locations. Predecessor hash at returned node's hash
// minus 1000.
func makeSimpleChord() *Chord {
	localhost := net.ParseIP("127.0.0.1")
	ch := &Chord{}
	ch.n = MakeNode(localhost, 8888)
	ch.predecessor = Node{localhost, 0, ch.n.Hash - 1000}
	ch.ftable = make([]Node, ftableSize)
	// Initialize lists
	for i := 0; i < len(ch.ftable); i++ {
		ch.ftable[i] = Node{localhost, 0, ch.ftableStart(i)}
	}

	return ch
}

func TestChordInRangeUnit(t *testing.T) {
	fmt.Println("Test: Chord inRange unit test ...")

	// Test true case
	var min, max, h UHash = 1, 10, 5
	if !inRange(h, min, max) {
		t.Fatalf("inRange failed for %d < %d < %d", min, h, max)
	}

	// Test false cases
	h = 0
	if inRange(h, min, max) {
		t.Fatalf("inRange failed for %d < %d < %d", min, h, max)
	}

	h = 30
	if inRange(h, min, max) {
		t.Fatalf("inRange failed for %d < %d < %d", min, h, max)
	}

	// Test wrap case
	if !inRange(h, max, min) {
		t.Fatalf("inRange failed for %d < %d < %d", max, h, min)
	}

	// Test wrap around min is before zero, max is after zero, h inbetween
	h = 2
	max = 5
	min = MaxUHash - 100
	if !inRange(h, min, max) {
		t.Fatalf("inRange failed for wrap around, where %d < %d < %d", min, h, max)
	}

	// test wrap around min is before zero, max is after zero, h outside range
	h = 5000
	if inRange(h, min, max) {
		t.Fatalf("inRange should have failed. Wrap around, where h outside range %d < %d < %d", min, h, max)
	}

	// test corner cases
	h = 0
	max = 0
	min = MaxUHash
	if !inRange(h, min, max) {
		t.Fatalf("inRange failed for %d < %d < %d", min, h, max)
	}

	h = 0
	max = 0
	min = 0
	if inRange(h, min, max) {
		t.Fatalf("inRange should have failed. min should be excluded %d < %d < %d", min, h, max)
	}

	fmt.Println(" ... Passed")
}

func TestChordFindClosestNodeUnit(t *testing.T) {
	fmt.Println("Test: Chord FindClosestNode unit test ...")

	ch := makeSimpleChord()

	// Check for something on the node
	h := ch.n.Hash - 10
	if n := ch.FindClosestNode(h); n.Hash != ch.n.Hash {
		t.Fatalf("FindClosestNode failed when node stored on self.\nIncorrectly returns %v instead %v",
			n.Hash, ch.n.Hash)
	}

	// Check for something on the successor
	h = ch.n.Hash + 1
	if n := ch.FindClosestNode(h); n.Hash != ch.ftable[0].Hash {
		t.Fatalf("FindClosestNode failed when node on successor")
	}

	// Check for something past last finger table entry
	h = ch.ftable[len(ch.ftable)-1].Hash + 100
	if n := ch.FindClosestNode(h); n.Hash != ch.ftable[len(ch.ftable)-1].Hash {
		t.Fatalf("FindClosestNode failed when node past last ftable entry")
	}

	// Check all other finger table entries
	for i := 1; i < len(ch.ftable); i++ {
		h = ch.ftable[i].Hash + 1
		n := ch.FindClosestNode(h)
		if n.Hash != ch.ftable[i].Hash {
			// Look up finger number that was given instead.
			idx := -1
			for j := 0; j < len(ch.ftable); j++ {
				if ch.ftable[j].Hash == n.Hash {
					idx = j
					break
				}
			}
			t.Fatalf("FindClosestNode failed for finger %d. Looking up %v. "+
				"Should be ch.ftable[%d]:%v. Got ch.ftable[%d]:%v",
				i, h, i, ch.ftable[i].Hash, idx, ch.ftable[idx].Hash)
		}
	}

	fmt.Println(" ... Passed")
}

// Initializes a chord instance from a node, all fields are initialized
// ftable, slist and predecessor need to be updated with proper values elsewhere
func initChordFromNode(n Node) *Chord {
	ch := &Chord{}
	ch.n = n
	ch.ftable = make([]Node, ftableSize)
	ch.isRunning = false
	ch.killStabilizeChan = nil
	ch.respChanMap = make(map[int]chan LookupResult)
	return ch
}

// Initializes a chord lookup ring of the desired size with hardcoded ftable
// and slist state. Returns the ring and rpc servers or an error.
func initializeLookupTestRing(size int) ([]*RPCServer, []*Chord, error) {
	if size <= 0 {
		err := fmt.Errorf("initializeChordLookupRing called with invalid size %d",
			size)
		return nil, nil, err
	}
	var err error
	localhost := net.ParseIP("127.0.0.1")
	basePort := 8888
	sharedKV := &KVServer{}
	rpcInstances := make([]*RPCServer, size)
	chordInstances := make([]*Chord, size)

	// Create nodes, chord instances and rpc instances
	for i := 0; i < size; i++ {
		n := MakeNode(localhost, basePort+i)
		tempCh := initChordFromNode(n)
		chordInstances[i] = tempCh
		rpcInstances[i], err = startRPC(tempCh, sharedKV, n.String())
		if err != nil {
			return nil, nil, err
		}
	}

	// Sort chordInstances by hash
	ns := &nodeSorter{&chordInstances}
	sort.Sort(ns)

	// for each chord in the list of sorted chord instances, update ftable slist
	// and predecessor
	for i, chInst := range chordInstances {
		// initialize predecessor
		chInst.predecessor = chordInstances[posMod(i-1, size)].n
		// initialize fingertables
		for j := range chInst.ftable {
			idx := findKeyOwner(&chordInstances, chInst.ftableStart(j))
			chInst.ftable[j] = chordInstances[idx].n
		}
	}
	return rpcInstances, chordInstances, nil
}

type testType bool

const (
	controlled testType = true
	random     testType = false
)

type LookupStats struct {
	totalLatency time.Duration
	aveLatency   time.Duration
	totalHops    int
	aveHops      float64
	numLookups   int
}

func printStats(stats *LookupStats) {
	fmt.Printf("\n\nLookup Statistics\n")
	fmt.Printf("\tNumber of Lookups:\n\t%d\n", stats.numLookups)
	fmt.Printf("\tTotal Number of Hops:\n\t%d\n", stats.totalHops)
	fmt.Printf("\tTotal Latency:\n\t%v\n", stats.totalLatency)
	fmt.Printf("\tAverage Number of Hops:\n\t%0.2f\n", stats.aveHops)
	fmt.Printf("\tAverage Latency:\n\t%v\n", stats.aveLatency)
	fmt.Printf("\n")
}

// Tests the chord ring on keys starting from random nodes trials number of
// times. Can test on totally random keys or keys whose successor is a desired
// node. It is assumed that chord ring is a sorted list of chord instances.
// Returns an error if less than 100% of the lookups pass.
func testLookups(numLookups int, chordInstances []*Chord, tType testType) (*LookupStats, error) {
	if numLookups <= 0 {
		return nil, fmt.Errorf("numLookups parameter invalid: %d", numLookups)
	}
	if chordInstances == nil {
		return nil, fmt.Errorf("chordInstances parameter invalid")
	}

	stats := &LookupStats{}
	var r = rand.New(rand.NewSource(time.Now().UnixNano()))
	var ringSize = len(chordInstances)
	numCorrect := 0
	for i := 0; i < numLookups; i++ {
		// Generate a random key based on testtype
		var key UHash
		var targetNodeHash UHash
		if tType == random {
			// Generate random key in between 0 and MaxUHash
			key = UHash(r.Float64() * MaxUHash)
		} else {
			// Randomly choose a target
			targetID := posMod(r.Int(), ringSize)
			targetNodeHash = chordInstances[targetID].n.Hash
			prevNodeHash := chordInstances[posMod(targetID-1, ringSize)].n.Hash
			keyRange := math.Abs(float64(targetNodeHash - prevNodeHash))

			// Let the key fall randomly between the targetNode and the node before
			// it (exclusive).
			key = (UHash(r.Float64()*keyRange) + prevNodeHash + 1) % MaxUHash
		}

		// Pick a random node
		idx := posMod(r.Int(), ringSize)
		tempCh := chordInstances[idx]

		// Lookup the key
		resultCh, info, err := tempCh.Lookup(key)
		if err != nil {
			return nil, err
		}

		// Find correct result
		correctCh := chordInstances[findKeyOwner(&chordInstances, key)]
		if tType == controlled && correctCh.n.Hash != targetNodeHash {
			return nil, fmt.Errorf("Test's correct answer computed incorrectly.")
		}

		// match, increment number of correct lookups
		if correctCh.n.Hash != resultCh.n.Hash {
			DPrintf("Chord[0x%016x].ftable:", tempCh.n.Hash)
			for i, a := range tempCh.ftable {
				DPrintf("ftable[%02d]: 0x%016x", i, a.Hash)
			}
			DPrintf("Failed on trial %d", numCorrect)
			return nil, fmt.Errorf("Chord[0x%016x].Lookup(0x%016x) failed. Expected: "+
				"0x%016x; Received: 0x%016x\n",
				tempCh.n.Hash, key, correctCh.n.Hash, resultCh.n.Hash)
		}

		fmt.Printf("\n[%d]: Lookup on Node[0x%016x]. Result[0x%016x]. Hops[%d]. Latency[%v]\n",
			i, tempCh.n.Hash, resultCh.n.Hash, info.Hops, info.Latency)

		stats.totalHops += info.Hops
		stats.totalLatency += info.Latency
		numCorrect++
	}

	// if any lookups were unsuccessful, return an error
	if numCorrect != numLookups {
		return nil, fmt.Errorf("Lookup Test Failed. Accuracy %v%%",
			float32(numCorrect)/float32(numLookups))
	}

	stats.numLookups = numLookups
	stats.aveHops = float64(stats.totalHops) / float64(numLookups)
	stats.aveLatency = stats.totalLatency / time.Duration(numLookups)

	return stats, nil
}

func ringHashesToString(chordInstances []*Chord) string {
	sBuf := new(bytes.Buffer)
	sBuf.WriteString("Hashes of nodes in chord ring:\n")
	for idx, ch := range chordInstances {
		sBuf.WriteString(fmt.Sprintf("\t%d: 0x%016x\n", idx, ch.n.Hash))
	}
	return sBuf.String()
}

// Code to test lookups within a chord ring. Where chord instances have
// fingertables, predecessor and successor information initialized by test code
func TestLookup(t *testing.T) {
	lookupType := "Iterative"
	if !isIterative {
		lookupType = "Recursive"
	}

	fmt.Printf("Test: Chord %s Lookup tests ...", lookupType)
	testSize := 200
	numLookups := 20

	var err error
	rpcss, ring, err := initializeLookupTestRing(testSize)
	if err != nil {
		sBuf := new(bytes.Buffer)
		sBuf.WriteString("\tInitializing chord lookup test ring failed\n")
		sBuf.WriteString(err.Error())
		t.Fatal(sBuf)
	}

	// Make sure servers are disabled. No need to kill chord instances as no
	// goroutines were started for them. Fields were filledin ("hardcoded")
	for _, rpcs := range rpcss {
		defer rpcs.End()
	}

	fmt.Println("\tChord Ring Initialized")

	fmt.Println("\tTesting Random Lookups")
	stats, err := testLookups(numLookups, ring, random)
	if err != nil {
		t.Fatalf("\tRandom Lookups Test failed for %d random lookups with error\n\t%s\n%s",
			numLookups, err.Error(), ringHashesToString(ring))
	}
	fmt.Printf("\tFinished testing ring with size %d with %d Random Lookups.\n", testSize, numLookups)
	printStats(stats)

	fmt.Println("\tTesting Controlled Lookups")
	stats, err = testLookups(numLookups, ring, controlled)
	if err != nil {
		t.Fatalf("\tControlled lookups Test failed for %d random lookups with error\n\t%s\n%s",
			numLookups, err.Error(), ringHashesToString(ring))
	}
	fmt.Printf("\tFinished testing ring with size %d with %d Controlled Lookups.\n", testSize, numLookups)
	printStats(stats)

	fmt.Println(" ... Passed")
}
