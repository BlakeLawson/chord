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
// the array index for the node or the closest node to that hash.
func findNearestNode(chords *[]*Chord, target UHash) int {
	size := len(*chords)
	if size <= 1 {
		return 0
	}

	for i := 0; i < size; i++ {
		if (*chords)[i].n.Hash == target {
			return i
		}
		if inRange(target, (*chords)[i].n.Hash, (*chords)[(i+1)%size].n.Hash) {
			return (i + 1) % size
		}
	}

	// This should never happen.
	return -1
}

// TODO: Figure out the right way to handle termination. Right now, the rpc
// server may shut down before the chord instance, and if the chord instance
// is in the process of stabilizing, then the chord instance panics and causing
// the test to fail.
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
	chordInstances[0], err = MakeChord(n, nil)
	if err != nil {
		return fmt.Errorf("Chord[0] initialization failed: %s", err)
	}
	defer chordInstances[0].Kill()

	rpcInstances[0], err = startRPC(chordInstances[0], sharedKV, n.String())
	if err != nil {
		return fmt.Errorf("RPCServer[0] initialization failed: %s", err)
	}
	defer rpcInstances[0].end()

	// Initialization phase.
	for i := 1; i < size; i++ {
		n = MakeNode(localhost, basePort+i)
		chordInstances[i], err = MakeChord(n, chordInstances[0].n)
		if err != nil {
			return fmt.Errorf("Chord[%d] initiailzation failed: %s", i, err)
		}
		defer chordInstances[i].Kill()

		rpcInstances[i], err = startRPC(chordInstances[i], sharedKV, n.String())
		if err != nil {
			return fmt.Errorf("RPCServer[%d] initialization failed: %s", i, err)
		}
		defer rpcInstances[i].end()
	}

	// Give time to stabilize
	time.Sleep(time.Duration(size) * time.Second)

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
		for j := 0; j < len(chordInstances[i].ftable); j++ {
			expectedIdx := findNearestNode(&chordInstances, chordInstances[i].fTableStart(j))
			if chordInstances[i].ftable[j].Hash != chordInstances[expectedIdx].n.Hash {
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
		t.Fatalf("Chord single initialization failed: %s", err)
	}

	fmt.Println(" ... Passed")
}

func TestChordThreeInitialization(t *testing.T) {
	fmt.Println("Test: Chord triple initialization ...")
	err := initializeChordRing(3)
	if err != nil {
		t.Fatalf("Chord single initialization failed: %s", err)
	}

	fmt.Println(" ... Passed")
}

// This function may give an error that says something along the lines of "too
// many open files". There's a way to adjust the maximum number of files you
// can have open at once, but Blake hasn't tried it yet.
func TestChordManyInitialization(t *testing.T) {
	fmt.Println("Test: Chord many initialization ...")
	err := initializeChordRing(20)
	if err != nil {
		t.Fatalf("Chord single initialization failed: %s", err)
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
	ch.predecessor = &Node{localhost, 0, ch.n.Hash - 1000}
	ch.ftable = make([]*Node, fTableSize)
	ch.slist = make([]*Node, sListSize)

	// Initialize lists
	for i := 0; i < len(ch.slist); i++ {
		ch.slist[i] = &Node{localhost, 0, ch.n.Hash + UHash(i)}
	}
	for i := 0; i < len(ch.ftable); i++ {
		ch.ftable[i] = &Node{localhost, 0, ch.fTableStart(i)}
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
func InitChordFromNode(n *Node) *Chord {
	ch := &Chord{}
	ch.n = n
	ch.isRunning = true
	ch.ftable = make([]*Node, fTableSize)
	ch.slist = make([]*Node, sListSize)
	ch.killChan = make(chan bool)
	ch.respChanMap = make(map[int]chan *Chord)
	return ch
}

// initializes a chord lookup ring of the desired size.
// returns the ring and rpc servers or an error
func initializeLookupTestRing(size int) ([]*RPCServer, []*Chord, error) {
	if size <= 0 {
		return nil, nil, fmt.Errorf("initializeChordLookupRing called with invalid size %d", size)
	}
	var err error
	localhost := net.ParseIP("127.0.0.1")
	basePort := 8888
	sharedKV := &KVServer{}
	rpcInstances := make([]*RPCServer, size)
	chordInstances := make([]*Chord, size)
	// Create nodes, chord insances and rpcinstances
	for i := 0; i < size; i++ {
		n := MakeNode(localhost, basePort+i)
		tempCh := InitChordFromNode(n)
		chordInstances[i] = tempCh
		rpcInstances[i], err = StartRPC(tempCh, sharedKV, basePort+i)
		if err != nil {
			return nil, nil, err
		}
	}
	//sort chordInstances by hash
	ns := &nodeSorter{&chordInstances}
	sort.Sort(ns)

	// for each chord in the list of sorted chord instances, update ftable slist
	// and predecessor
	for i, chInst := range chordInstances {
		// initialize predecessor
		prevIdx := posMod(i-1, size)
		chInst.predecessor = chordInstances[prevIdx].n

		// initialize successors
		// TODO: what if size / num chordInstances is less than successor list size
		for k := 0; k < sListSize; k++ {
			chInst.slist[k] = chordInstances[(i+k+1)%size].n
		}
		// initialize fingertables
		// find the first node in chordInstances that has a hash greater or equal to
		// fingerKStart, this hash will also be greater than the current node so we can use inRange.
		// TODO: handling corner cases of one or two nodes separately
		idx := (i + 1) % size // first possible chordinstance for ftable is nect one after this
		for k := range chInst.ftable {
			fingerKStart := chInst.fTableStart(k)
			found := false
			for !found {
				// when a node greater or equal to fingerKStart is found, add it to the fingertable
				if inRange(fingerKStart, fingerKStart-1, chordInstances[idx].n.Hash) {
					chInst.ftable[k] = chordInstances[idx].n
					found = true
					break
				}
				// else check if the next available chord instance is greater than the finger's hash
				idx = (idx + 1) % size
			}
		}
	}
	return rpcInstances, chordInstances, nil
}

// tests the chord ring on keys starting from random nodes trials number of times.
// can test on totally random keys or keys whose successor is a desired node
// it is assumed that chord ring is a sorted list of chord instances.
// returns an error if less than 100% of the lookups pass.
func testLookups(numLookups int, chordInstances []*Chord, testType bool) error {
	var r = rand.New(rand.NewSource(time.Now().UnixNano()))
	var ringSize = len(chordInstances)
	numCorrect := 0
	for i := 0; i < numLookups; i++ {
		// generate a random key based on testtype
		var key UHash
		var targetNodeHash UHash
		if testType == Random {
			//generate random key in between 0 and MaxUHash
			key = UHash(r.Float64() * MaxUHash)
		} else {
			// randomly choose a target
			targetID := posMod(r.Int(), ringSize)
			targetNodeHash = chordInstances[targetID].n.Hash
			prevNodeHash := chordInstances[posMod(targetID-1, ringSize)].n.Hash
			keyRange := math.Abs(float64(targetNodeHash - prevNodeHash))
			// let the key fall randomly between the targetNode and the node before it(exclusive).
			key = (UHash(r.Float64()*keyRange) + prevNodeHash + 1) % MaxUHash
		}

		// pick a random node
		idx := posMod(r.Int(), ringSize)
		tempCh := chordInstances[idx]
		// lookup the key
		resultCh, err := tempCh.Lookup(key)

		if err != nil {
			return err
		}

		var correctCh *Chord
		prev := ringSize - 1
		for curr := 0; curr < ringSize; curr++ {
			if inRange(key, chordInstances[prev].n.Hash, chordInstances[curr].n.Hash) {
				correctCh = chordInstances[curr]
				break
			}
			prev = curr
		}

		if testType == Controlled && correctCh.n.Hash != targetNodeHash {
			return fmt.Errorf("\tError in test Code.\n\tTargetNodeHash %v is not equal to calculatedNodeHash %v\n. key is %v",
				targetNodeHash, correctCh.n.Hash, key)
		}

		// match, increment number of correct lookups
		if correctCh.n.Hash == resultCh.n.Hash {
			//log.Printf("Match in Rand Lookup tests for key %v. Match found on node %v", key, resultCh.n.Hash)
			numCorrect++
		}

		if correctCh == nil {
			return fmt.Errorf("Issue in chord ring or test. key %v has no successor in chord ring", key)
		}
	}
	// if all lookups were successful, return no error
	if numCorrect == numLookups {
		return nil
	}

	return fmt.Errorf("Lookup Test Failed.  Accuracy %v %%", float32(numCorrect)/float32(numLookups))
}

func ringHashesToString(chordInstances []*Chord) string {
	sBuf := new(bytes.Buffer)
	sBuf.WriteString("\tThese are the hashes of the nodes in the chord ring\n")
	for idx, ch := range chordInstances {
		sBuf.WriteString(fmt.Sprintf("\t%d: %v\n", idx, ch.n.Hash))
	}
	return sBuf.String()
}

// Code to test lookups within a chord ring. Where chord instances have
// fingertables, predecessor and successor information initialized by test code
// TODO: add code to shutDown RPCServers when bug fixed
func TestLookup(t *testing.T) {
	fmt.Println("Test: Chord Lookup tests ...")

	var err error
	_, ring, err := initializeLookupTestRing(5)

	if err != nil {
		sBuf := new(bytes.Buffer)
		sBuf.WriteString("\tInitializing chord lookup test ring failed\n")
		sBuf.WriteString(err.Error())
		t.Fatal(sBuf)
	}
	fmt.Println("\tChord Ring Initialized")

	numLookups := 10
	fmt.Println("\tTesting Random Lookups")
	err = testLookups(numLookups, ring, Random)
	if err != nil {
		sBuf := new(bytes.Buffer)
		sBuf.WriteString(fmt.Sprintf("\tRandom Lookups Test failed for %d random lookups with error\n", numLookups))
		sBuf.WriteString(err.Error())
		sBuf.WriteString(ringHashesToString(ring))
		t.Fatal(sBuf)
	}
	fmt.Println("\tFinished testing Random Lookups.")

	fmt.Println("\tTesting Controlled Lookups")
	err = testLookups(numLookups, ring, Controlled)
	if err != nil {
		sBuf := new(bytes.Buffer)
		sBuf.WriteString(fmt.Sprintf("\tControlled Lookups Test failed for %d random lookups with error\n", numLookups))
		sBuf.WriteString(err.Error())
		sBuf.WriteString(ringHashesToString(ring))
		t.Fatal(sBuf)
	}
	fmt.Println("\tFinished testing Controlled Lookups.")
	fmt.Println(" ... Passed")
}
