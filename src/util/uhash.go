// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)
package util

import (
	"crypto/sha256"
	"encoding/binary"
)

// Uniform hash type.
type UHash uint64

// Hash the given string.
func Hash(v string) UHash {
	sum := sha256.Sum256([]byte(v))

	// Convert to 64 bit hash
	bytesPerUint64 := 8
	h, _ := binary.Uvarint(sum[:bytesPerUint64])
	return UHash(h)
}
