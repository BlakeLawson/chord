// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package chord

import (
	"sync"
	"util"
)

// Chord represents single Chord instance.
type Chord struct {
	mu          sync.Mutex
	isIterative bool

	// predecessor
	predecessor *util.Node

	// finger table
	ftable []*util.Node

	// successor list
	slist []*util.Node
}

func (ch *Chord) recursiveLookup(key string) (*util.Node, error) {
	return nil, nil
}

func (ch *Chord) iterativeLookup(key string) (*util.Node, error) {
	return nil, nil
}

//Lookup
func (ch *Chord) Lookup(key string) (*util.Node, error) {
	if ch.isIterative {
		return ch.iterativeLookup(key)
	}
	return ch.recursiveLookup(key)
}

// KeyRange returns start and end of key range this chord instance is
// responsible for. It is possible for the end of range to be lower value than
// the start of the range if the range wraps around key space.
func (ch *Chord) KeyRange() (uint64, uint64) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return uint64(ch.predecessor.Hash), uint64(ch.ftable[0].Hash)
}

// Make Chord object and join the Chord ring
func Make(existingNode *util.Node, isIterative bool) *Chord {
	return nil
}
