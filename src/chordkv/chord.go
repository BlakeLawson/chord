// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package chordkv

import (
	"sync"
)

const (
	// TODO: revisit this number
	sListSize int = 10
	fTableSize int = 64
)

// Chord represents single Chord instance.
type Chord struct {
	mu          sync.Mutex
	isIterative bool

	// predecessor
	predecessor *Node

	// finger table
	ftable []*Node

	// successor list
	slist []*Node
}

func (ch *Chord) recursiveLookup(key string) (*Node, error) {
	return nil, nil
}

func (ch *Chord) iterativeLookup(key string) (*Node, error) {
	return nil, nil
}

// Lookup node responsible for key. Returns the node and its predecessor.
func (ch *Chord) Lookup(key string) (*Node, error) {
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

// Make Chord object and join the Chord ring. If existingNode is null, then
// this Chord node is first.
func Make(self *Node, existingNode *Node, isIterative bool) (*Chord, error) {
	ch := &Chord{}
	ch.isIterative = isIterative
	ch.ftable = make([]*Node, fTableSize)
	ch.slist = make([]*Node, sListSize)

	return ch, nil
}
