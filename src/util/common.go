// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)
package util

import (
	"fmt"
	"log"
)

const debug = false

// Print functions

// Pretty logs
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

// Cprintf prints with colors. Only works in debugging mode.
func Cprintf(c Color, format string, a ...interface{}) {
	if debug {
		log.Printf("%s%s%s", c, fmt.Sprintf(format, a), None)
	}
}

// Dprintf is debugging print statement
func Dprintf(format string, a ...interface{}) {
	Cprintf(None, format, a)
}
