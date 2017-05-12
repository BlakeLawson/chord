// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package hyperviser

import (
	"fmt"
	"os"
	"testing"
)

const (
	testPass    = "abc"
	testLogName = "textOut"
)

var testLogDir = os.Getenv("GOPATH") + "/src/hyperviser/"

// Test Set up
func TestMain(m *testing.M) {
	// Configure to run tests with localhost addresses.
	serverAddrs = map[AddrPair]bool{
		AddrPair{"127.0.0.1", 8888}: true,
		AddrPair{"127.0.0.1", 8889}: true,
	}
	os.Exit(m.Run())
}

// Test basic initialization
func TestInitialization(t *testing.T) {
	fmt.Println("Test: Initialization ...")

	hvs := make([]*Hyperviser, len(serverAddrs))
	i := 0
	var err error
	for ap := range serverAddrs {
		hvs[i], err = makePort(ap.IP, testPass, testLogDir, ap.Port)
		if err != nil {
			t.Fatalf("Initializing hv %s failed: %s", ap.IP, err)
		}
		defer hvs[i].Stop(true)
	}

	fmt.Println(" ... Passed")
}
