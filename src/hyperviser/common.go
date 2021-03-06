// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package hyperviser

import (
	"fmt"
	"log"
)

const debug = true

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
		log.Printf("%s%s%s", c, fmt.Sprintf(format, a...), None)
	}
}

// DPrintf is debugging print statement
func DPrintf(format string, a ...interface{}) {
	if debug {
		log.Printf(format, a...)
	}
}

// calculates the remainder of two numbers, ensures it is positive
func posMod(x, y int) int {
	z := x % y
	if z < 0 {
		z = z + y
	}
	return z
}
