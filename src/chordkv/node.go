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
func MakeNode(addr net.IP, port int) Node {
	if addr == nil || port <= 0 {
		DPrintf("MakeNode: Invalid arguments addr:%v port:%d", addr, port)
	}

	n := Node{addr, port, 0}
	n.Hash = Hash(n.String())
	return n
}

// String returns string representation of n.
func (n *Node) String() string {
	return fmt.Sprintf("%s:%d", n.Addr.String(), n.Port)
}

// Equal returns true if nodes n and m are the same. Return false otherwise.
func (n *Node) Equal(m *Node) bool {
	if n == nil && m == nil {
		return true
	}
	if n == nil || m == nil {
		return false
	}
	return n.Addr.Equal(m.Addr) && n.Port == m.Port
}
