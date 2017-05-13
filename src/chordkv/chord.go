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
	isIterative        bool          = true
	sListSize          int           = 10
	ftableSize         int           = 64
	stabilizeTimeout   time.Duration = 250 * time.Millisecond
	updateSlistTimeout time.Duration = 200 * time.Millisecond
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

	// Channels used to terminate instance's background threads.
	killStabilizeChan   chan bool
	killUpdateSlistChan chan bool

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

	// recursively find correct node and send result.
	var wg sync.WaitGroup
	var err error
	wg.Add(1)

	go func(pwg *sync.WaitGroup) {
		defer pwg.Done()
		err = ch.ForwardLookup(h, ch, rID)
		if err != nil {
			DPrintf("chord [%016x]: forward lookup failed: %s", ch.n.Hash, err)
			respChan <- nil
		}
	}(&wg)

	// wait for result and then delete channel
	chRes := <-respChan
	wg.Wait()

	ch.mu.Lock()
	delete(ch.respChanMap, rID)
	ch.mu.Unlock()

	// if lookup fails return err
	if err != nil {
		return nil, err
	}

	return chRes, nil
}

// ForwardLookup forwards the lookup to an appropriate node closer to h. If this
// chord instance is responsible for h, it lets the source of the lookup know.
// TODO: should this be done in a go routine? This blocks when a request is forwarded until last node sens resul
func (ch *Chord) ForwardLookup(h UHash, source *Chord, rID int) error {
	// base case. if this node is responsible for h, let the source of the lookup know
	ch.mu.Lock()
	if inRange(h, ch.predecessor.Hash, ch.n.Hash) {
		ch.mu.Unlock()
		source.n.RemoteSendLookupResult(rID, ch)
		return nil
	}

	// looks up the node closest to h on ch's fingertable and forwards lookup to it
	dest, err := ch.FindClosestNode(h)
	if err != nil {
		return err
	}
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
	closest, err := ch.FindClosestNode(h)
	if err != nil {
		return nil, err
	}
	if ch.n.Hash == closest.Hash {
		ch.mu.Unlock()
		return ch, nil
	}
	ch.mu.Unlock()

	for {
		temp, rCh, err := closest.RemoteFindClosestNode(h)
		closest = temp
		if err != nil {
			return nil, fmt.Errorf("chord [%s]: RemoteFindClosestNode on %016x failed: %s",
				ch.n.String(), h, err)
		}
		if rCh != nil {
			return rCh, nil
		}
	}
}

// FindClosestNode is a helper function for lookups. It returns the closest
// node to the identifier h given this node's fingertable, even if it has failed
// Returns an error if every node it knows close to the identifier h has failed
// THIS METHOD ASSUMES THAT IT IS CALLED FROM A LOCKING CONTEXT.
// IT WOULD BE UNWISE TO CALL LOOKUP IN THIS METHOD EVEN FOR FAULT TOLERANCE
func (ch *Chord) FindClosestNode(h UHash) (*Node, error) {
	// If I am closest, return myself
	if inRange(h, ch.predecessor.Hash, ch.n.Hash) {
		return ch.n, nil
	}

	if inRange(h, ch.n.Hash, ch.ftable[0].Hash) {
		return ch.ftable[0], nil
	}

	// for all nodes, check if key h falls in range,
	var failedNode *Node
	trySuccessors := false
	for i := 0; i < ftableSize-1; i++ {
		node := ch.ftable[i]
		// if key falls in range
		if inRange(h, node.Hash, ch.ftable[i+1].Hash) {
			// ping node to see if it is alive
			err := node.RemotePing()
			// if no error return node
			if err == nil {
				return node, nil
			}
			failedNode = node
			// fixFinger
			go ch.fixFinger(i)
			// if there is an error, either return the node immediately before failed node or
			// if all these nodes fail or it was just successor that failed go to successor list
			for j := i - 1; j >= 0; j-- {
				tempNode := ch.ftable[j]
				err := tempNode.RemotePing()
				if err == nil {
					return tempNode, nil
				}
				go ch.fixFinger(i)
			}
			trySuccessors = true
		}

	}

	// try all appropriate successors to handle node failures
	if trySuccessors {
		prev := ch.n
		for _, successor := range ch.slist {
			if inRange(h, prev.Hash, successor.Hash) {
				err := successor.RemotePing()
				if err == nil {
					return successor, nil
				}
			}
			prev = successor
		}

		return failedNode, fmt.Errorf("All nodes that [%v] knows that could lead to %v have failed", ch.n.Hash, h)
	}

	// if not found near any node in finger table, return last node in ftable.
	// the lookup is in another area of they ring entirely
	return ch.ftable[ftableSize-1], nil
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
		// if predecessor dead, n, is probably your predecessor
	} else if err := ch.predecessor.RemotePing(); err != nil {
		ch.predecessor = n
		// else if it is alive, check if n is inrange
	} else if inRange(n.Hash, ch.predecessor.Hash, ch.n.Hash) {
		ch.predecessor = n
	}

	return nil
}

// Pick a random entry in the finger table and check whether it is up to date.
func (ch *Chord) fixFingers() error {
	i := rand.Intn(len(ch.ftable)-1) + 1
	err := ch.fixFinger(i)
	return err
}

// Pick a random entry in the finger table and check whether it is up to date.
func (ch *Chord) fixFinger(i int) error {
	ftCh, err := ch.Lookup(ch.ftableStart(i))
	if err != nil {
		return fmt.Errorf("error on %d: %s", i, err)
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
		case <-ch.killStabilizeChan:
			return
		case <-t.C:
			// Verify successor pointer up to date and update finger table.
			// TODO: Add fault tolerance with successor list.
			// TODO: Double check concurrency controls here.
			go func() {
				pSucc, err := ch.ftable[0].RemoteGetPred()
				if !ch.checkRunning() {
					return
				}
				// If successor is dead, call (check and) updateSuccessor
				if err != nil {
					err = ch.updateSuccessor()
					if err != nil {
						DPrintf("ch.stabilize [%s]: checkSuccessor failed: %s\n", ch.n.String(), err)
					}
				} else {
					ch.mu.Lock()
					if inRange(pSucc.Hash, ch.n.Hash, ch.ftable[0].Hash) {
						ch.ftable[0] = pSucc
						ch.slist[0] = pSucc
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

				err = ch.ftable[0].RemoteNotify(ch.n)
				if !ch.checkRunning() {
					return
				}
				if err != nil {
					DPrintf("chord [%s]: Notify(%s) failed: %s\n", ch.n.String(),
						ch.ftable[0].String(), err)
				}
			}()
		}
	}
}

// updateSuccessorlist periodicaly updates successor list to ensure it is correct
// given that we should know our immediate successor via stabilize also calls fixFingers
func (ch *Chord) updateSuccessorlist() {
	t := time.NewTicker(updateSlistTimeout)
	defer t.Stop()

	for {
		select {
		case <-ch.killUpdateSlistChan:
			return
		case <-t.C:
			go func() {
				ch.mu.Lock()
				ch.slist[0] = ch.ftable[0]
				succ := ch.slist[0]
				ch.mu.Unlock()

				// check if successor is alive, if not, update it in slist and ftable
				err := ch.updateSuccessor()
				if err != nil {
					DPrintf("ch.stabilize [%s]: checkSuccessor failed: %s\n", ch.n.String(), err)
				}

				if !ch.checkRunning() {
					return
				}

				// update the rest of the successor list
				for i := 1; i < sListSize; i++ {
					nextSucc, err := succ.RemoteGetSucc()
					// if this fails, lookupUp the would be nextSucc of succ
					if err != nil {
						nextSuccCh, err := ch.Lookup((succ.Hash + 1) % MaxUHash)

						// if RemoteLookup fails, handle silently by printing; successor is previous successor
						if err != nil {
							DPrintf("ch.updateSuccessorlist [%s]: Lookup(%s) failed: %s\n", ch.n.String(),
								(succ.Hash+1)%MaxUHash)
							nextSucc = ch.slist[i-1]
						} else {
							nextSucc = nextSuccCh.n
						}
						// update successor list
						ch.mu.Lock()
						ch.slist[i] = nextSucc
						succ = ch.slist[i]
						ch.mu.Unlock()

					} else {
						// normal case
						ch.mu.Lock()
						ch.slist[i] = nextSucc
						succ = nextSucc
						ch.mu.Unlock()
					}
					if !ch.checkRunning() {
						return
					}
					ch.fixFingers()
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

// UpdateFtable is an RPC endpoint called by other chord nodes that think they
// may belong in ch's ftable at entry i.
func (ch *Chord) UpdateFtable(n *Node, i int) error {
	if n == nil {
		return fmt.Errorf("UpdateFtable: nil node")
	}
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
func (ch *Chord) findPredecessor(h UHash) (*Node, error) {
	successor, err := ch.Lookup(h)
	if err != nil {
		return nil, err
	}
	return successor.predecessor, nil
}

// updateSuccessor updates the successor in ftable and slist if dead.
// If it can't find the successor, it returns an error.
func (ch *Chord) updateSuccessor() error {
	// should not be nil when system starts
	err := ch.ftable[0].RemotePing()
	if err != nil {
		// get successor's successor from slist notify it that this is its predecessor
		// if this fails, fail silently and print
		//log.Printf("\tchord [0x%016x] Successor [0x%016x] dead", ch.n.Hash, ch.ftable[0].Hash)

		var succ *Node
		for i := 1; i < sListSize; i++ {
			ch.mu.Lock()
			succ = ch.slist[i]
			ch.mu.Unlock()
			err = succ.RemotePing()
			if err == nil {
				break
			}
		}
		ch.mu.Lock()
		ch.slist[0] = succ
		ch.ftable[0] = succ
		ch.mu.Unlock()
		succ.RemoteNotify(ch.n)
	}
	return nil
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
	wg := new(sync.WaitGroup)
	for i := 0; i < ftableSize; i++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()

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
	wg.Wait()
}

// GetNode returns ch's network information.
func (ch *Chord) GetNode() Node {
	return *ch.n
}

// MakeChord creates object and join the Chord ring. If existingNode is null,
// then this Chord node is first.
func MakeChord(self *Node, existingNode *Node) (*Chord, error) {
	ch := &Chord{}
	ch.n = self
	ch.isRunning = true
	ch.ftable = make([]*Node, ftableSize)
	ch.slist = make([]*Node, sListSize)
	ch.killStabilizeChan = make(chan bool)
	ch.killUpdateSlistChan = make(chan bool)
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

		// Update other node's finger tables.
		ch.updateOthers()
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

	// Start Stabilize and updateSuccessorlist thread in the background.
	go ch.Stabilize()
	go ch.updateSuccessorlist()
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
		ch.killUpdateSlistChan <- true
	} else {
		ch.mu.Unlock()
	}
}
