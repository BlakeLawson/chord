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
	isIterative      bool          = true
	sListSize        int           = 10
	ftableSize       int           = 64
	stabilizeTimeout time.Duration = 250 * time.Millisecond
)

// Chord represents single Chord instance.
type Chord struct {
	mu sync.Mutex

	// True if instance has not been killed yet
	isRunning bool

	// Network information about this Chord instance.
	n Node

	// Predecessor in chord ring
	predecessor Node

	// finger table
	ftable []Node

	// Channels used to terminate instance's stabilize threads.
	killStabilizeChan chan bool

	// Tracks number of requests to generate unique IDs for respChanMap.
	reqCount int

	// Used to receive result of recursive lookup
	respChanMap map[int]chan LookupResult
}

// LookupInfo used to return performance information to user.
type LookupInfo struct {
	Hops    int
	Latency time.Duration
}

// LookupResult is response from other chord instance during recursive lookup.
type LookupResult struct {
	rID         int
	chordResult *Chord
	hops        int
	err         error
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
func (ch *Chord) recursiveLookup(h UHash) (*Chord, int, error) {
	// generate request ID, add channel to map and then forward lookup
	hops := 0
	ch.mu.Lock()
	rID := ch.reqCount
	ch.reqCount++
	respChan := make(chan LookupResult, 1)
	ch.respChanMap[rID] = respChan
	ch.mu.Unlock()

	DPrintf("ch [%016x]: recursiveLookup %d -> %016x", ch.n.Hash, rID, h)

	// recursively find correct node and send result.
	go ch.ForwardLookup(h, ch, rID, hops)

	// wait for result and then delete channel
	DPrintf("ch [%016x]: recursiveLookup %d -> %016x waiting on respChan",
		ch.n.Hash, rID, h)
	res := <-respChan

	DPrintf("ch [%016x]: recursiveLookup %d -> %016x done", ch.n.Hash, rID, h)

	ch.mu.Lock()
	delete(ch.respChanMap, rID)
	ch.mu.Unlock()

	return res.chordResult, res.hops, res.err
}

// ForwardLookup forwards the lookup to an appropriate node closer to h. If this
// chord instance is responsible for h, it lets the source of the lookup know.
func (ch *Chord) ForwardLookup(h UHash, source *Chord, rID, hops int) error {
	// Base case. If this node is responsible for h, let the source of the
	// lookup know.
	ch.mu.Lock()
	defer ch.mu.Unlock()

	DPrintf("ch [%016x]: ForwardLookup %d -> %016x: calling in range %016x -> %016x",
		ch.n.Hash, rID, h, ch.predecessor.Hash, ch.n.Hash)
	closest := ch.FindClosestNode(h)

	if ch.n.Hash != closest.Hash {
		hops++
		go func(n Node) {
			if err := closest.RemoteForwardLookup(h, source, rID, hops); err != nil {
				DPrintf("ch [%016x]: ForwardLookup %d -> %016x: RemoteForwardLookup "+
					"on %016x failed", n.Hash, rID, h, closest)
			}
		}(ch.n)
		return nil
	}

	DPrintf("ch [%016x]: ForwardLookup %d -> %016x: Found owner!!!",
		ch.n.Hash, rID, h)

	if ch.n.Hash == source.n.Hash {
		// Respond directly
		lookupChan, ok := ch.respChanMap[rID]
		if !ok {
			DPrintf("chord [%016x]: ForwardRequest %d -> %016x: request %d does not exist",
				ch.n.Hash, rID, h)
			return fmt.Errorf("chord [%016x]: request %d does not exist",
				ch.n.Hash, rID)
		}

		lookupChan <- LookupResult{rID, ch, hops, nil}
		delete(ch.respChanMap, rID)
	} else {
		go func(self, dest Node) {
			if err := dest.RemoteSendLookupResult(rID, hops, ch); err != nil {
				DPrintf("chord [%016x]: ForwardRequest %d -> %016x: "+
					"RemoteSendLookupResult on %016x failed", self.Hash, rID, h, dest.Hash)
			}
		}(ch.n, source.n)
	}
	return nil
}

// ReceiveLookUpResult receives lookup result and passes it to the respChanMap
func (ch *Chord) receiveLookUpResult(res LookupResult) error {
	ch.mu.Lock()
	lookupChan, ok := ch.respChanMap[res.rID]
	ch.mu.Unlock()
	if !ok {
		DPrintf("chord [%s]: receiveLookupResult: request %d does not exist",
			ch.n.String(), res.rID)
		return fmt.Errorf("chord [%s]: request %d does not exist", ch.n.String(), res.rID)
	}

	lookupChan <- res
	return nil
}

// iterativeLookup iteratively looks up the node responsible for identifier h
// and returns the number of successful hops it took to find it. Returns error on failure
func (ch *Chord) iterativeLookup(h UHash) (*Chord, int, error) {
	hops := 0
	ch.mu.Lock()
	closest := ch.FindClosestNode(h)

	if ch.n.Hash == closest.Hash {
		ch.mu.Unlock()
		return ch, hops, nil
	}
	ch.mu.Unlock()

	for {
		temp, rCh, err := closest.RemoteFindClosestNode(h)
		closest = temp
		if err != nil {
			return nil, 0, fmt.Errorf("chord [%s]: RemoteFindClosestNode on %016x failed: %s",
				ch.n.String(), h, err)
		}
		hops++
		if rCh.isRunning {
			return &rCh, hops, nil
		}
	}
}

// FindClosestNode is a helper function for lookups. It returns the closest
// node to the identifier h given this node's fingertable, even if it has failed
// Returns an error if every node it knows close to the identifier h has failed
// THIS METHOD ASSUMES THAT IT IS CALLED FROM A LOCKING CONTEXT.
// IT WOULD BE UNWISE TO CALL LOOKUP IN THIS METHOD EVEN FOR FAULT TOLERANCE
func (ch *Chord) FindClosestNode(h UHash) Node {
	// If I am closest, return myself
	if inRange(h, ch.predecessor.Hash, ch.n.Hash) {
		return ch.n
	}

	if inRange(h, ch.n.Hash, ch.ftable[0].Hash) {
		return ch.ftable[0]
	}

	// for all nodes, check if key h falls in range,
	for i := 0; i < ftableSize-1; i++ {
		node := ch.ftable[i]
		// if key falls in range
		if inRange(h, node.Hash, ch.ftable[i+1].Hash) {
			return node
		}
	}
	// if not found near any node in finger table, return last node in ftable.
	// the lookup is in another area of they ring entirely
	return ch.ftable[ftableSize-1]
}

// Lookup node responsible for key. Returns the node and its predecessor.
func (ch *Chord) Lookup(h UHash) (*Chord, *LookupInfo, error) {
	var chResult *Chord
	var hops int
	var err error
	start := time.Now()
	if isIterative {
		chResult, hops, err = ch.iterativeLookup(h)
	} else {
		chResult, hops, err = ch.recursiveLookup(h)
	}
	latency := time.Since(start)
	if err != nil {
		return nil, nil, err
	}
	return chResult, &LookupInfo{hops, latency}, nil
}

// Notify used to tell ch that n thinks it might be ch's predecessor.
func (ch *Chord) Notify(n Node) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.n.Hash == n.Hash {
		return nil
	}

	// Check whether ch is only node in ring
	if ch.n.Hash == ch.predecessor.Hash {
		DPrintf("ch [%016x]: Notify: setting predecessor to %016x",
			ch.n.Hash, n.Hash)
		// Update predecessor and successors.
		ch.predecessor = n
		ch.ftable[0] = n
	} else if inRange(n.Hash, ch.predecessor.Hash, ch.n.Hash) {
		DPrintf("ch [%016x]: Notify: setting predecessor to %016x",
			ch.n.Hash, n.Hash)
		ch.predecessor = n
	}

	return nil
}

// Pick a random entry in the finger table and check whether it is up to date.
func (ch *Chord) fixFingers() error {
	i := rand.Intn(ftableSize-1) + 1
	err := ch.fixFinger(i)
	return err
}

// Pick a specific entry in the finger table and check whether it is up to date.
func (ch *Chord) fixFinger(i int) error {
	ftCh, _, err := ch.Lookup(ch.ftableStart(i))
	if err != nil {
		return fmt.Errorf("error on %d: %s", i, err)
	}

	ch.mu.Lock()
	ch.ftable[i] = ftCh.n
	ch.mu.Unlock()
	return nil
}

// Verify successor pointer up to date and update finger table.
func (ch *Chord) stabilizeImpl() {
	ch.mu.Lock()
	n := ch.ftable[0]
	ch.mu.Unlock()

	pSucc, err := n.RemoteGetPred()

	ch.mu.Lock()
	if !ch.isRunning || ch.ftable[0].Hash != n.Hash {
		// Something changed.
		ch.mu.Unlock()
		return
	}
	ch.mu.Unlock()

	// If successor is dead, call (check and) updateSuccessor
	if err != nil {
		DPrintf("stabilize: chord [%016x]: RemoteGetPred on %016x failed: %s",
			ch.n.Hash, n.Hash, err)
	} else {
		ch.mu.Lock()
		if inRange(pSucc.Hash, ch.n.Hash, ch.ftable[0].Hash) {
			ch.ftable[0] = pSucc
		}
		ch.mu.Unlock()
	}

	err = ch.fixFingers()
	if !ch.checkRunning() {
		return
	}
	if err != nil {
		// Not sure how to handle this case. Going to fail loudly for now.
		DPrintf("chord [%s]: fixFingers() failed: %s\n",
			ch.n.String(), err)
	}

	ch.mu.Lock()
	succ := ch.ftable[0]
	ch.mu.Unlock()
	err = succ.RemoteNotify(ch.n)
	if !ch.checkRunning() {
		return
	}
	if err != nil {
		DPrintf("chord [%s]: Notify(%s) failed: %s\n",
			ch.n.String(), succ.String(), err)
	}
}

// Stabilize periodically verify that ch's successor pointer is correct and
// notify the successor that ch exists.
func (ch *Chord) Stabilize() {
	t := time.NewTicker(stabilizeTimeout)
	defer t.Stop()

	for {
		select {
		case <-ch.killStabilizeChan:
			return
		case <-t.C:
			go ch.stabilizeImpl()
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
	if min == max {
		return true
	}

	return false
}

// UpdateFtable is an RPC endpoint called by other chord nodes that think they
// may belong in ch's ftable at entry i.
func (ch *Chord) UpdateFtable(n Node, i int) error {
	if i < 0 || i >= ftableSize {
		return fmt.Errorf("UpdateFtable: Invalid ftable index %d", i)
	}

	ch.mu.Lock()
	if inRange(n.Hash, ch.ftableStart(i), ch.ftable[i].Hash) {
		ch.ftable[i] = n
		p := ch.predecessor
		ch.mu.Unlock()

		// Notify predecessor since its ftable may also change.
		if p.Hash != n.Hash {
			go func() {
				err := p.RemoteUpdateFtable(n, i)
				if err != nil {
					DPrintf("chord[%016x]: UpdateFtable: (%016x).RemoteUpdateFtable"+
						"(%016x, %d) failed: %s", ch.n.Hash, p.Hash, n.Hash, i, err)
				}
			}()
		}
	} else {
		ch.mu.Unlock()
	}

	return nil
}

// findPredecessor finds the predecessor for the given key.
func (ch *Chord) findPredecessor(h UHash) (Node, error) {
	successor, _, err := ch.Lookup(h)
	if err != nil {
		return Node{}, err
	}
	return successor.predecessor, nil
}

// priorFtableOwner returns the UHash of a node that may have ch in its ftable
// at entry i.
func (ch *Chord) priorFtableOwner(i int) UHash {
	x := new(big.Int).SetUint64(uint64(ch.n.Hash))
	y := new(big.Int).SetUint64(uint64(math.Pow(2, float64(i))))
	diff := new(big.Int).Sub(x, y)

	// Ensure result is mod MaxUint64
	if diff.Sign() < 0 {
		diff = diff.Add(diff, new(big.Int).SetUint64(math.MaxUint64))
	}

	return UHash(diff.Uint64())
}

// updateOthers notifies other chord nodes that they need to update their
// finger tables.
func (ch *Chord) updateOthers() {
	for i := 0; i < ftableSize; i++ {
		go func(j int) {
			// Calculate node that would have ch in ftable entry j.
			owner := ch.priorFtableOwner(j)

			// Find nearest responsible node.
			pred, err := ch.findPredecessor(owner)
			if err != nil {
				DPrintf("chord[%016x]: findPredecessor(%016x) failed: %s",
					ch.n.Hash, owner, err)
				return
			}

			if pred.Hash != ch.n.Hash {
				// Update node's ftable.
				err = pred.RemoteUpdateFtable(ch.n, j)
				if err != nil {
					DPrintf("chord[%016x]: (%016x).RemoteUpdateFtable(%016x, %d) failed: %s",
						ch.n.Hash, pred.Hash, ch.n.Hash, j)
				}
			}
		}(i)
	}
}

// GetNode returns ch's network information.
func (ch *Chord) GetNode() Node {
	return ch.n
}

// MakeChord creates object and join the Chord ring. If existingNode is null,
// then this Chord node is first.
func MakeChord(self, existingNode Node, isFirst bool) (*Chord, error) {
	DPrintf("ch [%x]: MakeChord", self.Hash)
	ch := &Chord{}
	ch.n = self
	ch.isRunning = true
	ch.ftable = make([]Node, ftableSize)
	ch.killStabilizeChan = make(chan bool)
	ch.respChanMap = make(map[int]chan LookupResult)

	// Initialize slist and ftable
	if !isFirst {
		// Use existing node to initialize.
		DPrintf("ch [%016x]: MakeChord: calling lookup on self", self.Hash)
		successor, _, err := existingNode.RemoteLookup(self.Hash)
		if err != nil {
			return nil, err
		}
		DPrintf("ch [%016x]: MakeChord: found successor = %016x",
			self.Hash, successor.n.Hash)
		ch.ftable[0] = successor.n
		ch.predecessor = successor.predecessor
		DPrintf("ch [%016x]: MakeChord: initializing ftable", self.Hash)
		for i := 0; i < len(successor.ftable); i++ {
			// TODO: Initialize with successor's ftable to improve performance.
			var ftCh *Chord
			ftCh, _, err = existingNode.RemoteLookup(ch.ftableStart(i))
			if err != nil {
				return nil, err
			}
			ch.ftable[i] = ftCh.n
		}

		DPrintf("ch [%016x]: notifying successor %s", self.Hash, successor.n.String())
		err = successor.n.RemoteNotify(ch.n)
		if err != nil {
			log.Fatalf("chord [%016x]: initial notify failed: %s", ch.n.Hash, err)
		}

		// Update other node's finger tables.
		DPrintf("ch [%016x]: calling updateOthers", self.Hash)
		ch.updateOthers()
	} else {
		// No other nodes in the ring.
		ch.predecessor = ch.n
		for i := 0; i < len(ch.ftable); i++ {
			ch.ftable[i] = ch.n
		}
	}

	// Start Stabilize thread in the background.
	go ch.Stabilize()
	DPrintf("ch [%016x]: MakeChord: returning success", ch.n.Hash)
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
		ch.killStabilizeChan <- true
	} else {
		ch.mu.Unlock()
	}
}

// GetID return's ch's identifier.
func (ch *Chord) GetID() UHash {
	return ch.n.Hash
}
