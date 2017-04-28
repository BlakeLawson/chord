// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package chordkv

import (
	"fmt"
	"net"
	// "sort"
	"testing"
)

// nodeSorter used to sort array of nodes by hash.
type nodeSorter struct {
	chords []*Chord
}

// Len is part of sort.Interface.
func (ns *nodeSorter) Len() int {
	return len(ns.chords)
}

// Swap is part of sort.Interface.
func (ns *nodeSorter) Swap(i, j int) {
	ns.chords[i], ns.chords[j] = ns.chords[j], ns.chords[i]
}

// Less is part of sort.Interface.
func (ns *nodeSorter) Less(i, j int) bool {
	return ns.chords[i].n.Hash < ns.chords[j].n.Hash
}

func initializeChordRing(size int) error {
	if size <= 0 {
		return fmt.Errorf("initializeChordRing called with invalid size %d", size)
	}

	// rpcInstances := make([]*RPCServer, size)
	// chordInstances := make([]*Chord, size)

	// Initialization phase.
	for i := 1; i < size; i++ {

	}

	// Validation phase.
	// ns := &nodeSorter{chordInstances}
	// sort.Sort(ns)
	for i := 0; i < size; i++ {

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
		t.Fatalf("inRange failed for wrap around. where h outside range %d < %d < %d", min, h, max)
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
		h = ch.ftable[i].Hash
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
