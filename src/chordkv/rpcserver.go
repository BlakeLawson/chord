// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package chordkv

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

// RPCServer used to handle client RPC requests.
type RPCServer struct {
	mu           sync.Mutex
	ch           *Chord
	kv           *KVServer
	servListener net.Listener
	baseServ     *rpc.Server
	running      bool
	errChan      chan error
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
	ChFields ChordFields
}

// ChordLookup returns result of lookup performed by the Chord instance running
// on this RPCServer.
func (rpcs *RPCServer) ChordLookup(args *ChordLookupArgs, reply *ChordLookupReply) error {
	rCh, err := rpcs.ch.Lookup(args.H)
	if err != nil {
		return err
	}

	reply.ChFields = *serializeChord(rCh)
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
	H UHash
}

// FindClosestReply holds reply to FindClosestReply.
type FindClosestReply struct {
	N        Node
	ChFields *ChordFields
}

// FindClosestNode finds the closest node to a hash from the Chord instance on this server.
func (rpcs *RPCServer) FindClosestNode(args *FindClosestArgs, reply *FindClosestReply) error {
	reply.ChFields = nil

	// TODO: Not happy about doing locking here
	rpcs.ch.mu.Lock()
	reply.N = *rpcs.ch.FindClosestNode(args.H)
	rpcs.ch.mu.Unlock()
	if rpcs.ch.n.Hash == reply.N.Hash {
		reply.ChFields = serializeChord(rpcs.ch)
	}
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

func serializeChord(ch *Chord) *ChordFields {
	return &ChordFields{
		N:           ch.n,
		Predecessor: ch.predecessor,
		FTable:      ch.ftable,
		SList:       ch.slist}
}

func deserializeChord(chFields *ChordFields) *Chord {
	if chFields == nil {
		return nil
	}
	return &Chord{
		n:           chFields.N,
		predecessor: chFields.Predecessor,
		ftable:      chFields.FTable,
		slist:       chFields.SList}
}

// ForwardLookupArgs holds arguments for FindClosestNode.
type ForwardLookupArgs struct {
	H        UHash
	RID      int
	ChFields ChordFields
}

// ForwardLookupReply holds reply to is an empty interface.
type ForwardLookupReply interface{}

// ForwardLookup finds the closest node to a hash from the Chord instance on this server.
func (rpcs *RPCServer) ForwardLookup(args *ForwardLookupArgs, reply *ForwardLookupReply) error {
	source := deserializeChord(&args.ChFields)
	err := rpcs.ch.ForwardLookup(args.H, source, args.RID)
	if err != nil {
		return err
	}
	return nil
}

// Ping used for testing
func (rpcs *RPCServer) Ping(args struct{}, reply *struct{}) error {
	return nil
}

// Return true if server is up. Return false otherwise.
func (rpcs *RPCServer) isRunning() bool {
	rpcs.mu.Lock()
	defer rpcs.mu.Unlock()
	return rpcs.running
}

// StartRPC creates an RPCServer listening on given port.
func StartRPC(ch *Chord, kv *KVServer, port int) (*RPCServer, error) {
	return startRPC(ch, kv, fmt.Sprintf(":%d", port))
}

// Local start method with more control over address server listens on. Used
// for testing.
func startRPC(ch *Chord, kv *KVServer, addr string) (*RPCServer, error) {
	rpcs := &RPCServer{}
	rpcs.ch = ch
	rpcs.kv = kv
	rpcs.errChan = make(chan error, 1)
	rpcs.baseServ = rpc.NewServer()

	if err := rpcs.baseServ.Register(rpcs); err != nil {
		return nil, fmt.Errorf("rpc registration failed: %s", err)
	}

	var err error
	rpcs.servListener, err = net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	// Start thread for server.
	go func() {
		rpcs.mu.Lock()
		rpcs.running = true
		rpcs.mu.Unlock()

		// Blocks until completion
		rpcs.baseServ.Accept(rpcs.servListener)

		rpcs.mu.Lock()
		rpcs.errChan <- nil
		rpcs.running = false
		rpcs.mu.Unlock()
	}()

	return rpcs, nil
}

// Block until server stops.
func (rpcs *RPCServer) wait() error {
	if !rpcs.isRunning() {
		return nil
	}

	return <-rpcs.errChan
}

// End the server if it is running. Returns nil on success.
func (rpcs *RPCServer) end() error {
	if !rpcs.isRunning() {
		return nil
	}

	if err := rpcs.servListener.Close(); err != nil {
		return fmt.Errorf("error closing listener: %s", err)
	}

	return nil
}
