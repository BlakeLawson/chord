// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package port

import (
	"fmt"
	"net"
	"testing"
)

// TestNew uses port.New to get a port and tries to listen on that port.
func TestNew(t *testing.T) {
	fmt.Println("Test: Finding new port ...")

	p, err := New()
	if err != nil {
		t.Fatalf("New failed: %s", err)
	}

	conn, err := net.Listen("tcp", fmt.Sprintf(":%d", p))
	if err != nil {
		t.Fatalf("Listening on new port failed")
	}

	conn.Close()

	fmt.Println(" ... Passed")
}

// TestCheck makes sure that port.Check identifies port in use.
func TestCheck(t *testing.T) {
	fmt.Println("Test: Check port state ...")

	// Find a port to listen on
	p := 8888
	var conn net.Listener
	for {
		var err error
		conn, err = net.Listen("tcp", fmt.Sprintf(":%d", p))
		if err == nil {
			break
		}
		p++
	}
	defer conn.Close()

	if status, _ := Check(p); status != false {
		t.Fatalf("Check returned wrong status")
	}

	fmt.Println(" ... Passed")
}

// TestNewCheck uses both port.New and port.Check.
func TestNewCheck(t *testing.T) {
	fmt.Println("Test: using both New and Check ...")

	p, err := New()
	if err != nil {
		t.Fatalf("New failed: %s", err)
	}

	status, err := Check(p)
	if err != nil {
		t.Fatalf("Check failed: %s", err)
	}
	if status != true {
		t.Fatalf("Check returned wrong status")
	}

	fmt.Println(" ... Passed")
}
