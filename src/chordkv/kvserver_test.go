// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package chordkv

import (
	"fmt"
	"net"
	"port"
	"testing"
)

func TestKVServerBasic(t *testing.T) {
	fmt.Println("Test: Basic KVServer ops ...")

	p, err := port.New()
	if err != nil {
		t.Fatalf("port.New failed: %s", err)
	}

	n := MakeNode(net.ParseIP("127.0.0.1"), p)
	ch, err := MakeChord(n, Node{}, true)
	if err != nil {
		t.Fatalf("Chord initialziation failed")
	}
	defer ch.Kill()
	kvs := MakeKVServer(ch)

	// Test basic put/get.
	k := "abc"
	v := "def"
	kvs.Put(k, v)
	if kvs.Get(k) != v {
		t.Fatalf("Basic put/get failed")
	}

	// Test overwriting.
	v = "aa"
	kvs.Put(k, v)
	if kvs.Get(k) != v {
		t.Fatalf("Overwrite value failed")
	}

	// Test StateSize method
	if kvs.StateSize() != 1 {
		t.Fatalf("State Size method failed")
	}

	fmt.Println(" ... Passed")
}
