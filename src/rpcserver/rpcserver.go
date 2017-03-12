// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package rpcserver

import (
	"chord"
	"fmt"
	"kvserver"
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

// RPCServer used to handle client RPC requests.
type RPCServer struct {
	ch *chord.Chord
	kv *kvserver.KVServer
}

// KVGetArgs holds arguments to KVGet RPC.
type KVGetArgs struct {
	Key string
}

// KVGetReply holds reply to KVGet RPC.
type KVGetReply struct {
	Val string
}

// KVGet returns result of get on KVServer running on this RPCServer.
func (rpcs *RPCServer) KVGet(args *KVGetArgs, reply *KVGetReply) error {
	reply.Val = rpcs.kv.Get(args.Key)
	return nil
}

// KVPutArgs holds arguments to KVPut RPC.
type KVPutArgs struct {
	Key string
	Val string
}

// KVPutReply is a placeholder for KVPut RPC's return values since KVPut only
// needs to return an error.
type KVPutReply interface{}

// KVPut performs put on KVServer running on this RPCServer.
func (rpcs *RPCServer) KVPut(args *KVPutArgs, reply *KVPutReply) error {
	rpcs.kv.Put(args.Key, args.Val)
	return nil
}

// ChordLookupArgs holds arguments to ChordLookup RPC.
type ChordLookupArgs struct {
	Key string
}

// ChordLookupReply holds reply to ChordLookup RPC.
type ChordLookupReply struct {
	Addr net.IP
	Port int
}

// ChordLookup returns result of lookup performed by the Chord instance running
// on this RPCServer.
func (rpcs *RPCServer) ChordLookup(args *ChordLookupArgs, reply *ChordLookupReply) error {
	n, err := rpcs.ch.Lookup(args.Key)
	if err != nil {
		return err
	}

	reply.Addr = n.Addr
	reply.Port = n.Port
	return nil
}

// Package level variables to ensure only single server.
var serverMutex sync.Mutex
var serverListener net.Listener
var serverRunning bool
var serverInitialized bool

// Start RPCServer listening on given port. Does not return until error or
// program ends. It is an error to call Start more than once.
func Start(ch *chord.Chord, kv *kvserver.KVServer, port int) error {
	return start(ch, kv, fmt.Sprintf(":%d", port))
}

// Local start method with more control over address server listens on. Used
// for testing.
func start(ch *chord.Chord, kv *kvserver.KVServer, addr string) error {
	// Ensure only initialized once
	serverMutex.Lock()
	if serverRunning {
		serverMutex.Unlock()
		return fmt.Errorf("RPCServer: server already running")
	}
	serverRunning = true
	serverMutex.Unlock()
	defer func() {
		serverMutex.Lock()
		serverRunning = false
		serverMutex.Unlock()
	}()

	// Set up the RPC handlers.
	if !serverInitialized {
		serverInitialized = true
		rpcs := &RPCServer{ch, kv}
		rpc.Register(rpcs)
		rpc.HandleHTTP()
	}

	var err error
	serverListener, err = net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	// Run the server.
	err = http.Serve(serverListener, nil)

	// Ignore acceptable error message (occurs on End()).
	baseMsg := "accept tcp %s: use of closed network connection"
	okMsg1 := fmt.Sprintf(baseMsg, fmt.Sprintf("[::]%s", addr))
	okMsg2 := fmt.Sprintf(baseMsg, addr)
	if err != nil && err.Error() != okMsg1 && err.Error() != okMsg2 {
		return err
	}
	return nil
}

// End the server if it is running. Returns nil on success.
func End() error {
	serverMutex.Lock()
	defer serverMutex.Unlock()
	if !serverRunning {
		return fmt.Errorf("RPCServer: server not running")
	}

	return serverListener.Close()
}
