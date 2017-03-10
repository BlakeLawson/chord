// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)
package chord

import (
	"crypto/sha256"
	"net"
	"sync"
)

type Chord struct {
	mu          sync.Mutex
	isIterative bool

	// predecessor
	predecessor *net.IP

	// finger table
	ftable []*net.IP

	// successor list
	slist []*net.IP
}

func hash(v string) {

}

func (ch *Chord) recursiveLookup(key string) (net.IP, error) {

}

func (ch *Chord) iterativeLookup(key string) (net.IP, error) {

}

func (ch *Chord) Lookup(key string) (net.IP, error) {
	if ch.isIterative {
		return iterativeLookup(key)
	}
	return recursiveLookup(key)
}

// KeyRange returns start and end of key range this chord instance is
// responsible for.
func (ch *Chord) KeyRange() (uint64, uint64) {

}

// Make Chord object and join the Chord ring
func Make(isIterative bool) *Chord {

}
