// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package chordkv

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func TestInitialization(t *testing.T) {
	fmt.Println("Test: RPCServer initialization ...")

	// End server before running should fail.
	if err := End(); err == nil {
		t.Fatalf("ended server that wasn't running.")
	}

	// Create a server
	port := 8888
	ch, err := MakeChord(MakeNode(net.ParseIP("127.0.0.1"), port), nil, true)
	if err != nil {
		t.Fatalf("Chord initializtion failed")
	}
	defer ch.Kill()
	kv := MakeKVServer(ch)

	serverDone := make(chan bool)
	go func() {
		if err := Start(ch, kv, port); err != nil {
			t.Fatalf("server failed: %s", err)
		}
		serverDone <- true
	}()

	// Wait a few seconds to be sure that the server is running.
	time.Sleep(time.Second)

	// Try creating a new server while the first one is running
	testWorked := make(chan bool)
	go func() {
		if err := Start(ch, kv, port); err == nil {
			t.Fatalf("server started a second time.")
		}
		testWorked <- true
	}()

	select {
	case <-time.After(time.Second):
		t.Fatalf("server started a second time.")
	case <-testWorked:
		// Do nothing because it worked.
	}

	// Turn off server.
	if err := End(); err != nil {
		t.Fatalf("error stopping server: %s", err)
	}

	// Ensure server stopped.
	select {
	case <-time.After(time.Second):
		t.Fatalf("server did not stop")
	case <-serverDone:
		// Do nothing because it worked.
	}

	fmt.Println(" ... Passed")
}

func TestBasicRequest(t *testing.T) {
	fmt.Println("Test: RPCServer basic request ...")

	addr := "127.0.0.1"
	port := 8888
	ch, err := MakeChord(MakeNode(net.ParseIP(addr), port), nil, true)
	if err != nil {
		t.Fatalf("Chord initialization failed")
	}
	defer ch.Kill()
	kv := MakeKVServer(ch)

	go func() {
		if err := start(ch, kv, fmt.Sprintf("%s:%d", addr, port)); err != nil {
			t.Fatalf("server failed: %s", err)
		}
	}()

	// Let server start.
	time.Sleep(time.Second)

	// Try connecting to the server.
	n := MakeNode(net.ParseIP(addr), port)
	if _, err := RemoteGet(n, ""); err != nil {
		t.Fatalf("RPC failed")
	}

	// Stop the server.
	End()

	fmt.Println(" ... Passed")
}
