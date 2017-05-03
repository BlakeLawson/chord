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

// TODO: make sure it is impossible for values to have MaxUHash
// as when you mod by MaxUHash you get numbers less than it

// Hash the given string.
func Hash(v string) UHash {
	sum := sha256.Sum256([]byte(v))
	return UHash(binary.LittleEndian.Uint64(sum[:]))
}
