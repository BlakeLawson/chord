// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package hyperviser

import (
	"fmt"
	"os"
	"port"
	"testing"
)

const (
	testPass      = "abc"
	testLogName   = "out"
	leaderLogName = "leader"
)

// Fix path and create the test directory in the test.
var testLogDir = os.Getenv("GOPATH") + "/src/hyperviser/logs/"

func setup() {
	// Configure to run tests with localhost addresses.
	p1, _ := port.New()
	p2, _ := port.New()

	serverAddrs = map[AddrPair]bool{
		AddrPair{"127.0.0.1", p1}: true,
		AddrPair{"127.0.0.1", p2}: true,
	}
}

// Test basic initialization
func TestInitialization(t *testing.T) {
	fmt.Println("Test: Initialization ...")
	setup()

	hvs := make([]*Hyperviser, len(serverAddrs))
	i := 0
	fmt.Printf("len(serverAddres): %d\n", len(serverAddrs))
	var err error
	for ap := range serverAddrs {
		fmt.Printf("Using %s\n", ap.String())
		hvs[i], err = makePort(ap.IP, testPass, testLogDir, ap.Port)
		if err != nil {
			t.Fatalf("Initializing hv %s failed: %s", ap.String(), err)
		}
		defer hvs[i].Stop(true)
	}

	fmt.Println(" ... Passed")
}

// Basic test execution.
func TestHelloWorld(t *testing.T) {
	fmt.Println("Test: Hello World ...")
	setup()

	fmt.Printf("len(serverAddres): %d\n", len(serverAddrs))
	hvs := make([]*Hyperviser, len(serverAddrs))
	i := 0
	var err error
	for ap := range serverAddrs {
		fmt.Printf("Using %s\n", ap.String())
		hvs[i], err = makePort(ap.IP, testPass, testLogDir, ap.Port)
		if err != nil {
			t.Fatalf("Initializing hv %s failed: %s", ap.String(), err)
		}
		defer hvs[i].Stop(true)
	}

	fmt.Println("About to call StartLeader")
	err = hvs[0].StartLeader(HelloWorld, leaderLogName, testLogName)
	if err != nil {
		t.Fatalf("Hello World failed: %s", err)
	}

	fmt.Println(" .. Passed")
}
