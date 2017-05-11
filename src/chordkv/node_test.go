// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package chordkv

import (
	"fmt"
	"net"
	"testing"
)

func TestNode(t *testing.T) {
	fmt.Println("Test: node operations ...")

	addr := net.ParseIP("192.168.0.0")
	goodPort := 8888
	badPort := -1

	// Check error case
	n := MakeNode(nil, goodPort)
	if n != nil {
		t.Fatalf("Allowed nil IP address")
	}

	n = MakeNode(addr, badPort)
	if n != nil {
		t.Fatalf("Allowed negative port")
	}

	// Valid cases
	n = MakeNode(addr, goodPort)
	if n == nil {
		t.Fatalf("Failed to make node from valid address and port")
	}

	// Check hash
	m := MakeNode(addr, goodPort)
	if m.Hash != n.Hash {
		t.Fatalf("Nodes hashed to different values")
	}

	// Check Equal
	if !n.Equal(m) {
		t.Fatalf("Node equality failed")
	}
}
