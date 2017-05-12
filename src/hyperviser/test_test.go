// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package hyperviser

import (
	"fmt"
	"os"
	"testing"
)

const (
	testPass = "abc"
	testLogName = "textOut"
)

var testLogDir = os.Getenv("GOPATH") + "/src/hyperviser/"

// Test Set up
func TestMain(m *testing.M) {
	// Configure to run tests with localhost addresses.
	serverIPs = map[string]bool{
		"127.0.0.1": true,
		"::1": true,
	}
	os.Exit(m.Run())
}

// Test basic initialization
func TestInitialization(t *testing.T) {
	fmt.Println("Test: Initialization ...")

	hvs := make([]*Hyperviser, len(serverIPs))
	i := 0
	var err error
	for ip := range serverIPs {
		hvs[i], err = Make(ip, testPass, testLogDir)
		if err != nil {
			t.Fatalf("Initializing hv %s failed: %s", ip, err)
		}
		defer hvs[i].Stop(true)
	}

	fmt.Println(" ... Passed")
}
