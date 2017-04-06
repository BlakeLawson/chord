// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package chordkv

import (
	"fmt"
	"net"
)

// Node in Chord ring.
type Node struct {
	Addr net.IP
	Port int
	Hash UHash
}

// MakeNode initializes a node. Return nil to indicate error.
func MakeNode(addr net.IP, port int) *Node {
	if addr == nil || port <= 0 {
		return nil
	}

	n := &Node{addr, port, 0}
	n.Hash = Hash(n.String())
	return n
}

func (n *Node) String() string {
	return fmt.Sprintf("%s:%d", n.Addr.String(), n.Port)
}
