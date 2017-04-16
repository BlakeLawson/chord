// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package chordkv

import (
	"errors"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"
)

const (
	// TODO: revisit these numbers
	sListSize        int           = 10
	fTableSize       int           = 64
	stabilizeTimeout time.Duration = 500 * time.Millisecond
)

// Chord represents single Chord instance.
type Chord struct {
	mu          sync.Mutex
	isIterative bool

	// Network information about this Chord instance.
	n *Node

	// Predecessor in chord ring
	predecessor *Node

	// finger table
	ftable []*Node

	// successor list
	slist []*Node

	// Used to terminate instance's background threads.
	killChan chan bool

	// Used to receive result of recursive lookup
	lookupResultChan map[int]chan *Node
}

// Given index in fTable, return starting hash value
func (ch *Chord) fTableStart(i int) UHash {
	tmp := float64(ch.n.Hash) + math.Pow(2, float64(i))
	return UHash(math.Mod(tmp, math.MaxUint64))
}

// recursiveLookup recursively looks up the node responsible for identifier h
func (ch *Chord) recursiveLookup(h UHash) (*Chord, error) {
	// generate request ID, add channel to map and then forward lookup
	rID := getID()
	ch.lookupResultChan[rID] = make(chan *Node)
	ch.ForwardLookup(h, ch.n, rID)
	// wait for result and then delete channel
	n := <-ch.lookupResultChan[rID]
	delete(ch.lookupResultChan, rID)
	return &Chord{sync.Mutex{}, ch.isIterative, n, nil, nil, nil, nil, nil}, nil
}

// ForwardLookup forwards the lookup to an appropriate node closer to h. If this
// chord instance is responsible for h, it lets the source of the lookup know
// if this is t
func (ch *Chord) ForwardLookup(h UHash, source *Node, rID int) error {
	// base case. if this node is responsible for h, let the source of the lookup know
	min, max := ch.KeyRange()
	if inRange(h, UHash(min), UHash(max)) {
		RemoteSendLookupResult(source, rID, ch.n)
	}
	// else
	// looks up the node closest to h on ch's fingertable and forwards lookup to it
	dest := ch.FindClosestNode(h)
	err := RemoteForwardLookup(h, source, rID, dest)
	return err
}

// ReceiveLookUpResult receives lookup result and passes it to the lookupResultChan
func (ch *Chord) receiveLookUpResult(n *Node, rID int) error {
	lookupChan, ok := ch.lookupResultChan[rID]
	if !ok {
		return errors.New("This request does not exist")
	}
	lookupChan <- n
	return nil
}

// iterativeLookup iteratively looks up the node responsible for identifier h
func (ch *Chord) iterativeLookup(h UHash) (*Chord, error) {
	min, max := ch.KeyRange()
	if inRange(h, UHash(min), UHash(max)) {
		return ch, nil
	}

	closest := ch.FindClosestNode(h)

	for closest.Hash != h {
		temp, err := RemoteFindClosestNode(h, closest)
		if err != nil {
			continue
		}
		closest = temp
	}

	return &Chord{sync.Mutex{}, ch.isIterative, closest, nil, nil, nil, nil, nil}, nil
}

// FindClosestNode is a helper function for lookups. It returns the
// the closest node to the identifier h given this node's fingertable.
func (ch *Chord) FindClosestNode(h UHash) *Node {
	// could locking be problematic?
	prevKey := ch.n.Hash

	// for all nodes, check if key h falls in range,
	for _, node := range ch.ftable {
		if inRange(h, prevKey, node.Hash) {
			return node
		}
		prevKey = node.Hash
	}
	// if not found in finger table, return last node in ftable.
	return ch.ftable[fTableSize-1]
}

// Lookup node responsible for key. Returns the node and its predecessor.
func (ch *Chord) Lookup(h UHash) (*Chord, error) {
	if ch.isIterative {
		return ch.iterativeLookup(h)
	}
	return ch.recursiveLookup(h)
}

// Notify used to tell ch that n thinks it might be ch's predecessor.
func (ch *Chord) Notify(n *Node) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return nil
}

// Pick a random entry in the finger table and check whether it is up to date.
// THIS METHOD ASSUMES THAT IT IS CALLED FROM A LOCKING CONTEXT.
func (ch *Chord) fixFingers() error {
	i := rand.Intn(len(ch.ftable)-1) + 1
	ftCh, err := ch.Lookup(ch.fTableStart(i))
	if err != nil {
		return err
	}

	ch.ftable[i] = ftCh.n
	return nil
}

// Stabilize periodically verify that ch's successor pointer is correct and
// notify the successor that ch exists.
func (ch *Chord) Stabilize() {
	t := time.NewTicker(stabilizeTimeout)
	defer t.Stop()

	for {
		select {
		case <-ch.killChan:
			return
		case <-t.C:
			// Verify successor pointer up to date and update finger table.
			// TODO: Add fault tolerance with successor list
			pSucc, err := RemoteGetPred(ch.ftable[0])
			if err != nil {
				log.Fatalf("chord [%s]: successor lookup failed: %s\n", ch.n, err)
			}

			ch.mu.Lock()
			if ch.n.Hash < pSucc.Hash && pSucc.Hash < ch.ftable[0].Hash {
				ch.ftable[0] = pSucc
				ch.slist[0] = pSucc
			}
			err = ch.fixFingers()
			if err != nil {
				// Not sure how to handle this case. Going to fail loudly for now.
				log.Fatalf("chord [%s]: fixFingers() failed: %s\n", ch.n, err)
			}
			ch.mu.Unlock()

			err = ch.Notify(ch.ftable[0])
			if err != nil {
				// TODO: This should fail quietly, but going to throw fatal for now.
				// Change to DPrintf(...) later.
				log.Fatalf("chord [%s]: Notify(%s) failed: %s\n", ch.n, ch.ftable[0], err)
			}
		}
	}
}

// KeyRange returns start and end of key range this chord instance is
// responsible for. It is possible for the end of range to be lower value than
// the start of the range if the range wraps around key space.
func (ch *Chord) KeyRange() (uint64, uint64) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return uint64(ch.predecessor.Hash), uint64(ch.ftable[0].Hash)
}

// MakeChord creates object and join the Chord ring. If existingNode is null,
// then this Chord node is first.
func MakeChord(self *Node, existingNode *Node, isIterative bool) (*Chord, error) {
	ch := &Chord{}
	ch.isIterative = isIterative
	ch.n = self
	ch.ftable = make([]*Node, fTableSize)
	ch.slist = make([]*Node, sListSize)
	ch.killChan = make(chan bool)

	// Initialize slist and ftable
	if existingNode != nil {
		// Use existing node to initialize.
		successor, err := RemoteLookup(existingNode, self.Hash)
		if err != nil {
			return nil, err
		}

		ch.ftable[0] = successor.n
		ch.slist[0] = successor.n
		ch.predecessor = successor.predecessor
		for i := 0; i < len(successor.ftable); i++ {
			// TODO: Initialize with successor's ftable to improve performance.
			ftCh, err := RemoteLookup(existingNode, ch.fTableStart(i))
			if err != nil {
				return nil, err
			}
			ch.ftable[i] = ftCh.n
		}

		for i := 0; i < len(ch.slist); i++ {
			ch.slist[i+1] = successor.slist[i]
		}
	} else {
		// No other nodes in the ring.
		ch.predecessor = ch.n
		for i := 0; i < len(ch.ftable); i++ {
			ch.ftable[i] = ch.n
		}
		for i := 0; i < len(ch.slist); i++ {
			ch.slist[i] = ch.n
		}
	}

	// Start Stabilize thread in the background.
	go ch.Stabilize()
	return ch, nil
}

// Kill disables the given Chord instance.
func (ch *Chord) Kill() {
	ch.killChan <- true
}
