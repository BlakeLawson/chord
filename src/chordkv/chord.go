// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package chordkv

import (
	"fmt"
	"log"
	"math"
	"math/big"
	"math/rand"
	"sync"
	"time"
)

const (
	// TODO: revisit these numbers
	isIterative      bool          = true
	sListSize        int           = 10
	fTableSize       int           = 64
	stabilizeTimeout time.Duration = 75 * time.Millisecond
)

// Chord represents single Chord instance.
type Chord struct {
	mu sync.Mutex

	// True if instance has not been killed yet
	isRunning bool

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

	// Tracks number of requests to generate unique IDs for respChanMap.
	reqCount int

	// Used to receive result of recursive lookup
	respChanMap map[int]chan *Chord
}

// Given index in ftable, return starting hash value.
func (ch *Chord) ftableStart(i int) UHash {
	x := new(big.Int).SetUint64(uint64(ch.n.Hash))
	y := new(big.Int).SetUint64(uint64(math.Pow(2, float64(i))))
	sum := new(big.Int).Add(x, y)
	bigIdx := new(big.Int).Mod(sum, new(big.Int).SetUint64(math.MaxUint64))
	return UHash(bigIdx.Uint64())
}

// recursiveLookup recursively looks up the node responsible for identifier h
func (ch *Chord) recursiveLookup(h UHash) (*Chord, error) {
	// generate request ID, add channel to map and then forward lookup
	ch.mu.Lock()
	rID := ch.reqCount
	ch.reqCount++
	respChan := make(chan *Chord)
	ch.respChanMap[rID] = respChan
	ch.mu.Unlock()

	ch.ForwardLookup(h, ch, rID)

	// wait for result and then delete channel
	chRes := <-respChan

	ch.mu.Lock()
	delete(ch.respChanMap, rID)
	ch.mu.Unlock()
	return chRes, nil
}

// ForwardLookup forwards the lookup to an appropriate node closer to h. If this
// chord instance is responsible for h, it lets the source of the lookup know.
func (ch *Chord) ForwardLookup(h UHash, source *Chord, rID int) error {
	// base case. if this node is responsible for h, let the source of the lookup know
	ch.mu.Lock()
	if inRange(h, ch.predecessor.Hash, ch.ftable[0].Hash) {
		ch.mu.Unlock()
		source.n.RemoteSendLookupResult(rID, ch)
		return nil
	}

	// looks up the node closest to h on ch's fingertable and forwards lookup to it
	dest := ch.FindClosestNode(h)
	ch.mu.Unlock()
	return dest.RemoteForwardLookup(h, source, rID)
}

// ReceiveLookUpResult receives lookup result and passes it to the respChanMap
func (ch *Chord) receiveLookUpResult(result *Chord, rID int) error {
	ch.mu.Lock()
	lookupChan, ok := ch.respChanMap[rID]
	ch.mu.Unlock()
	if !ok {
		return fmt.Errorf("chord [%s]: request %d does not exist", ch.n.String(), rID)
	}
	lookupChan <- result
	return nil
}

// iterativeLookup iteratively looks up the node responsible for identifier h
func (ch *Chord) iterativeLookup(h UHash) (*Chord, error) {
	ch.mu.Lock()
	closest := ch.FindClosestNode(h)
	if ch.n.Hash == closest.Hash {
		ch.mu.Unlock()
		return ch, nil
	}
	ch.mu.Unlock()

	for {
		temp, rCh, err := closest.RemoteFindClosestNode(h)
		closest = temp
		if err != nil {
			return nil, fmt.Errorf("chord [%s]: RemoteFindClosestNode on %v failed. Error: %v ", ch.n.String(), h, err)
		}
		if rCh != nil {
			return rCh, nil
		}
	}
}

// FindClosestNode is a helper function for lookups. It returns the closest
// node to the identifier h given this node's fingertable. THIS METHOD ASSUMES
// THAT IT IS CALLED FROM A LOCKING CONTEXT.
func (ch *Chord) FindClosestNode(h UHash) *Node {
	// If I am closest, return myself
	if inRange(h, ch.predecessor.Hash, ch.n.Hash) {
		return ch.n
	}

	if inRange(h, ch.n.Hash, ch.ftable[0].Hash) {
		return ch.ftable[0]
	}

	// for all nodes, check if key h falls in range,
	for i := 0; i < fTableSize-1; i++ {
		node := ch.ftable[i]
		if inRange(h, node.Hash, ch.ftable[i+1].Hash) {
			return node
		}
	}

	// if not found in finger table, return last node in ftable.
	return ch.ftable[fTableSize-1]
}

// Lookup node responsible for key. Returns the node and its predecessor.
func (ch *Chord) Lookup(h UHash) (*Chord, error) {
	if isIterative {
		return ch.iterativeLookup(h)
	}
	return ch.recursiveLookup(h)
}

// Notify used to tell ch that n thinks it might be ch's predecessor.
func (ch *Chord) Notify(n *Node) error {
	if n == nil {
		return fmt.Errorf("chord [%s]: Notify called with nil node", ch.n.String())
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()

	// Check whether ch is only node in ring
	if ch.n.Hash == ch.predecessor.Hash {
		// Update predecessor and successors.
		ch.predecessor = n
		ch.ftable[0] = n
		ch.slist[0] = n
	} else if inRange(n.Hash, ch.predecessor.Hash, ch.n.Hash) {
		ch.predecessor = n
	}

	return nil
}

// Pick a random entry in the finger table and check whether it is up to date.
func (ch *Chord) fixFingers() error {
	i := rand.Intn(len(ch.ftable)-1) + 1
	ftCh, err := ch.Lookup(ch.ftableStart(i))
	if err != nil {
		return err
	}

	ch.mu.Lock()
	ch.ftable[i] = ftCh.n
	ch.mu.Unlock()
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
			// TODO: Add fault tolerance with successor list.
			// TODO: Double check concurrency controls here.
			go func() {
				pSucc, err := ch.ftable[0].RemoteGetPred()
				if err != nil && !ch.checkRunning() {
					log.Fatalf("chord [%s]: successor lookup failed: %s\n", ch.n.String(), err)
				}

				ch.mu.Lock()
				if inRange(pSucc.Hash, ch.n.Hash, ch.ftable[0].Hash) {
					ch.ftable[0] = pSucc
					ch.slist[0] = pSucc
				}
				ch.mu.Unlock()

				err = ch.fixFingers()
				if err != nil && !ch.checkRunning() {
					// Not sure how to handle this case. Going to fail loudly for now.
					log.Fatalf("chord [%s]: fixFingers() failed: %s\n", ch.n.String(), err)
				}

				err = ch.ftable[0].RemoteNotify(ch.n)
				if err != nil && !ch.checkRunning() {
					DPrintf("chord [%s]: Notify(%s) failed: %s\n", ch.n.String(), ch.ftable[0].String(), err)
				}
			}()
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

// inRange checks if a key is within min (exclusive) and max (inclusive) on the chord ring.
// TODO: what if a node is both min and max? how does that affect in range?
func inRange(key UHash, min UHash, max UHash) bool {
	// if this node respons
	if min < max && key > min && key <= max {
		return true
	}
	if min > max {
		if key > min && key <= MaxUHash {
			return true
		}
		if key >= 0 && key <= max {
			return true
		}
	}
	return false
}

// MakeChord creates object and join the Chord ring. If existingNode is null,
// then this Chord node is first.
func MakeChord(self *Node, existingNode *Node) (*Chord, error) {
	ch := &Chord{}
	ch.n = self
	ch.isRunning = true
	ch.ftable = make([]*Node, fTableSize)
	ch.slist = make([]*Node, sListSize)
	ch.killChan = make(chan bool)
	ch.respChanMap = make(map[int]chan *Chord)

	// Initialize slist and ftable
	if existingNode != nil {
		// Use existing node to initialize.
		successor, err := existingNode.RemoteLookup(self.Hash)
		if err != nil {
			return nil, err
		}

		ch.ftable[0] = successor.n
		ch.slist[0] = successor.n
		ch.predecessor = successor.predecessor
		for i := 0; i < len(successor.ftable); i++ {
			// TODO: Initialize with successor's ftable to improve performance.
			ftCh, err := existingNode.RemoteLookup(ch.ftableStart(i))
			if err != nil {
				return nil, err
			}
			ch.ftable[i] = ftCh.n
		}

		for i := 1; i < len(ch.slist); i++ {
			ch.slist[i] = successor.slist[i-1]
		}
		err = successor.n.RemoteNotify(ch.n)
		if err != nil {
			log.Fatalf("chord [%s]: initial notify failed: %s", ch.n.String(), err)
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

// Return true if this chord instance is running. Else return false to
// indicate that it has been killed.
func (ch *Chord) checkRunning() bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.isRunning
}

// Kill disables the given Chord instance.
func (ch *Chord) Kill() {
	ch.mu.Lock()
	if ch.isRunning {
		ch.isRunning = false
		ch.mu.Unlock()
		ch.killChan <- true
	} else {
		ch.mu.Unlock()
	}
}
