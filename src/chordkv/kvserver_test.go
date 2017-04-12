// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package chordkv

import (
	"fmt"
	"testing"
)

func TestBasic(t *testing.T) {
	fmt.Println("Test: Basic KVServer ops ...")

	ch, err := MakeChord(nil, nil, true)
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

	fmt.Println(" ... Passed")
}
