// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package chordkv

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func TestRPCServerInitialization(t *testing.T) {
	fmt.Println("Test: RPCServer initialization ...")

	ch := &Chord{}
	kv := &KVServer{}
	port := 8888
	serverDone := make(chan bool)

	// Create a server
	rpcs, err := StartRPC(ch, kv, port)
	fmt.Println("Just started server")
	if err != nil {
		t.Fatalf("Server initilization failed: %s", err)
	}
	go func() {
		if err := rpcs.Wait(); err != nil {
			t.Fatalf("Server failed: %s", err)
		}
		serverDone <- true
	}()

	// Wait a second to be sure that the server is running.
	time.Sleep(time.Second)

	// Turn off server.
	fmt.Println("closing server")
	if err := rpcs.End(); err != nil {
		t.Fatalf("Error stopping server: %s", err)
	}

	// Ensure server stopped.
	select {
	case <-time.After(time.Second):
		t.Fatalf("Server did not stop")
	case <-serverDone:
		// Do nothing because it worked.
	}

	fmt.Println(" ... Passed")
}

func TestRPCServerBasicRequest(t *testing.T) {
	fmt.Println("Test: RPCServer basic request ...")

	addr := "127.0.0.1"
	port := 8889
	ch := &Chord{}
	kv := &KVServer{}
	serverDone := make(chan bool)

	rpcs, err := startRPC(ch, kv, fmt.Sprintf("%s:%d", addr, port))
	if err != nil {
		t.Fatalf("Server initialization failed: %s", err)
	}

	go func() {
		if err := rpcs.Wait(); err != nil {
			t.Fatalf("Server failed: %s", err)
		}
		serverDone <- true
	}()

	// Let server start.
	time.Sleep(time.Second)

	// Try connecting to the server.
	n := MakeNode(net.ParseIP(addr), port)
	if err := n.RemotePing(); err != nil {
		t.Fatalf("RPC failed: %s", err)
	}

	// Stop the server.
	if err := rpcs.End(); err != nil {
		t.Fatalf("Error stopping server: %s", err)
	}

	select {
	case <-time.After(time.Second):
		t.Fatalf("Server did not stop")
	case <-serverDone:
		// Do nothing because it worked
	}

	fmt.Println(" ... Passed")
}

func TestRPCServerMultiple(t *testing.T) {
	fmt.Println("Test: Multiple RPCServers ...")

	N := 5
	addr := net.ParseIP("127.0.0.1")
	basePort := 8890
	ch := &Chord{}
	kv := &KVServer{}
	nodes := make([]Node, N)
	rpcss := make([]*RPCServer, N)
	serverDones := make([]chan bool, N)
	var err error

	// Initialize servers.
	for i := 0; i < N; i++ {
		nodes[i] = MakeNode(addr, basePort+i)
		serverDones[i] = make(chan bool)
		rpcss[i], err = startRPC(ch, kv, nodes[i].String())
		if err != nil {
			t.Fatalf("server%d failed to start: %s", i, err)
		}
		go func(j int) {
			if err := rpcss[j].Wait(); err != nil {
				t.Fatalf("server%d failed: %s", j, err)
			}
			serverDones[j] <- true
		}(i)
	}

	// Ping all servers
	for i := 0; i < N; i++ {
		go func(j int) {
			if err := nodes[j].RemotePing(); err != nil {
				t.Fatalf("server%d ping failed: %s", j, err)
			}
		}(i)
	}

	// Give things time to complete
	time.Sleep(time.Second)

	// Turn off the servers
	for i := 0; i < N; i++ {
		if err := rpcss[i].End(); err != nil {
			t.Fatalf("server%d end failed: %s", i, err)
		}
	}

	// Check that everything stopped
	tick := time.After(2 * time.Second)
	for i := 0; i < N; i++ {
		select {
		case <-tick:
			t.Fatalf("server%d failed to stop", i)
		case <-serverDones[i]:
			// Do nothing because it worked.
		}
	}

	fmt.Println(" ... Passed")
}
