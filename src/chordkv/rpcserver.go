// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package chordkv

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

// RPCServer used to handle client RPC requests.
type RPCServer struct {
	ch *Chord
	kv *KVServer
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
	H UHash
}

// ChordLookupReply holds reply to ChordLookup RPC.
type ChordLookupReply struct {
	N           *Node
	Predecessor *Node
	FTable      []*Node
	SList       []*Node
}

// ChordLookup returns result of lookup performed by the Chord instance running
// on this RPCServer.
func (rpcs *RPCServer) ChordLookup(args *ChordLookupArgs, reply *ChordLookupReply) error {
	rCh, err := rpcs.ch.Lookup(args.H)
	if err != nil {
		return err
	}

	reply.N = rCh.n
	reply.Predecessor = rCh.predecessor
	reply.FTable = rCh.ftable
	reply.SList = rCh.slist
	return nil
}

// GetPredArgs holds arguments for GetPred.
type GetPredArgs interface{}

// GetPredReply holds reply to GetPred.
type GetPredReply struct {
	N Node
}

// GetPred returns the predecessor of the Chord instance on this server.
func (rpcs *RPCServer) GetPred(args *GetPredArgs, reply *GetPredReply) error {
	reply.N = *rpcs.ch.predecessor
	return nil
}

// FindClosestArgs holds arguments for FindClosestNode.
type FindClosestArgs struct {
	h UHash
}

// FindClosestReply holds reply to FindClosestReply.
type FindClosestReply struct {
	N           *Node
	Predecessor *Node
	FTable      []*Node
	SList       []*Node
}

// FindClosestNode finds the closest node to a hash from the Chord instance on this server.
func (rpcs *RPCServer) FindClosestNode(args *FindClosestArgs, reply *FindClosestReply) error {
	rCh, err := rpcs.ch.FindClosestNode(args.h)
	if err != nil {
		return err
	}
	reply.N = rCh.n
	reply.Predecessor = rCh.predecessor
	reply.FTable = rCh.ftable
	reply.SList = rCh.slist
	return nil
}

// ChordFields holds values for building a chord instance.
// It is used to make passing around chord isntance information easier
type ChordFields struct {
	N           *Node
	Predecessor *Node
	FTable      []*Node
	SList       []*Node
}

func (chFields ChordFields) getChordInstance() *Chord {
	ch := &Chord{
		n:           chFields.N,
		predecessor: chFields.Predecessor,
		ftable:      chFields.FTable,
		slist:       chFields.SList,
	}
	return ch
}

// ForwardLookupArgs holds arguments for FindClosestNode.
type ForwardLookupArgs struct {
	h            UHash
	sourceFields ChordFields
	rID          int
}

// ForwardLookupReply holds reply to is an empty interface.
type ForwardLookupReply interface {
}

// ForwardLookup finds the closest node to a hash from the Chord instance on this server.
func (rpcs *RPCServer) ForwardLookup(args *ForwardLookupArgs, reply *ForwardLookupReply) error {
	source := args.sourceFields.getChordInstance()
	err := rpcs.ch.ForwardLookup(args.h, source, args.rID)
	if err != nil {
		return err
	}
	return nil
}

// Package level variables to ensure only single server.
var serverMutex sync.Mutex
var serverListener net.Listener
var serverRunning bool
var serverInitialized bool

// Start RPCServer listening on given port. Does not return until error or
// program ends. It is an error to call Start more than once.
func Start(ch *Chord, kv *KVServer, port int) error {
	return start(ch, kv, fmt.Sprintf(":%d", port))
}

// Local start method with more control over address server listens on. Used
// for testing.
func start(ch *Chord, kv *KVServer, addr string) error {
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
