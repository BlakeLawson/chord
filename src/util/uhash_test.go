// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)
package util

import (
	"fmt"
	"testing"
)

func TestHash(t *testing.T) {
	fmt.Println("Test: basic hashing ...")

	a := Hash("abc")
	b := Hash("abc")
	if a != b {
		t.Fatalf("Unequal hashes")
	}

	b = Hash("abb")
	if a == b {
		t.Fatalf("equal hashes")
	}

	fmt.Println(" ... passed")
}
