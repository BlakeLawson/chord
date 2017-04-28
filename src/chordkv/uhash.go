// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package chordkv

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
)

// UHash is a Uniform hash type.
type UHash uint64

// MaxUHash is the maximum hash of UHash
const MaxUHash = math.MaxUint64

// Hash the given string.
func Hash(v string) UHash {
	sum := sha256.Sum256([]byte(v))

	// Convert to 64 bit hash
	bytesPerUint64 := 8
	h, _ := binary.Uvarint(sum[:bytesPerUint64])
	return UHash(h)
}
