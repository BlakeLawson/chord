// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package chordkv

import (
	"fmt"
	"log"
	"math"
)

const debug = false

// Print functions

// Color used for pretty logs
type Color string

const (
	None   Color = "\033[0m"
	Red    Color = "\033[0;31m"
	Green  Color = "\033[0;32m"
	Blue   Color = "\033[0;34m"
	Yellow Color = "\033[1;33m"
	Gray   Color = "\033[1;30m"
	White  Color = "\033[1;37m"
)

// CPrintf prints with colors. Only works in debugging mode.
func CPrintf(c Color, format string, a ...interface{}) {
	if debug {
		log.Printf("%s%s%s", c, fmt.Sprintf(format, a), None)
	}
}

// DPrintf is debugging print statement
func DPrintf(format string, a ...interface{}) {
	CPrintf(None, format, a)
}

//InRange checks if a key is within min (exclusive) and max (inclusive) on the chord ring.
func inRange(key UHash, min UHash, max UHash) bool {
	// if this node respons
	if min < max && key > min && key <= max {
		return true
	}
	if min > max {
		if key > min && key <= math.MaxInt64 {
			return true
		}
		if key >= 0 && key <= max {
			return true
		}
	}
	return false
}