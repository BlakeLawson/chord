// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package hyperviser

import (
	"fmt"
	"os"
	"port"
	"testing"
)

const (
	testPass = "abc"
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
	var err error
	for ap := range serverAddrs {
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

	testLogName := "helloOut"
	leaderLogName := "helloLeader"

	hvs := make([]*Hyperviser, len(serverAddrs))
	i := 0
	var err error
	for ap := range serverAddrs {
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

// Test the performance test.
func TestLookupPerf(t *testing.T) {
	fmt.Println("Test: lookupPerf ...")
	setup()

	testLogName := "perfOut"
	leaderLogName := "perfLeader"

	hvs := make([]*Hyperviser, len(serverAddrs))
	i := 0
	var err error
	for ap := range serverAddrs {
		hvs[i], err = makePort(ap.IP, testPass, testLogDir, ap.Port)
		if err != nil {
			t.Fatalf("Initializing hv %s failed: %s", ap.String(), err)
		}
		defer hvs[i].Stop(true)
	}

	err = hvs[0].StartLeader(LookupPerf, leaderLogName, testLogName)
	if err != nil {
		t.Fatalf("lookupPerf failed: %s", err)
	}

	fmt.Println(" ... Passed")
}
