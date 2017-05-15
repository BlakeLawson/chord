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
	initBarrier  sync.RWMutex
	ch           *Chord
	kv           *KVServer
	servListener net.Listener
	baseServ     *rpc.Server
	running      bool
	errChan      chan error
}

// checkInit used at the start of every RPC to ensure that the server does
// not respond to requests until the server is fully initialized.
func (rpcs *RPCServer) checkInit() {
	rpcs.initBarrier.RLock()
	rpcs.initBarrier.RUnlock()
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
	rpcs.checkInit()
	reply.Val = rpcs.kv.Get(args.Key)
	return nil
}

// KVPutArgs holds arguments to KVPut RPC.
type KVPutArgs struct {
	Key string
	Val string
}

// KVPutReply is a placeholder for KVPut RPC's reply  since KVPut only
// needs to return an error.
type KVPutReply interface{}

// KVPut performs put on KVServer running on this RPCServer.
func (rpcs *RPCServer) KVPut(args *KVPutArgs, reply *KVPutReply) error {
	rpcs.checkInit()
	rpcs.kv.Put(args.Key, args.Val)
	return nil
}

// KVSizeArgs is a placeholder for KVSize RPC's args
type KVSizeArgs interface{}

// KVSizeReply holds reply to KVSize RPC.
type KVSizeReply struct {
	Size int
}

// KVSize returns result of StateSize on KVServer running on this RPCServer.
func (rpcs *RPCServer) KVSize(args *KVSizeArgs, reply *KVSizeReply) error {
	reply.Size = rpcs.kv.StateSize()
	return nil
}

// ChordLookupArgs holds arguments to ChordLookup RPC.
type ChordLookupArgs struct {
	H UHash
}

// ChordLookupReply holds reply to ChordLookup RPC.
type ChordLookupReply struct {
	ChFields ChordFields
	Info     LookupInfo
}

// ChordLookup returns result of lookup performed by the Chord instance running
// on this RPCServer.
func (rpcs *RPCServer) ChordLookup(args *ChordLookupArgs, reply *ChordLookupReply) error {
	rpcs.checkInit()
	DPrintf("ch [%016x]: ChordLookup for %016x", rpcs.ch.n.Hash, args.H)
	rCh, info, err := rpcs.ch.Lookup(args.H)
	if err != nil {
		return err
	}

	reply.ChFields = *serializeChord(rCh)
	reply.Info = *info
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
	DPrintf("ch [%016x]: GetPred called", rpcs.ch.n.Hash)
	rpcs.checkInit()
	DPrintf("ch [%016x]: GetPred waiting on rpc lock", rpcs.ch.n.Hash)
	rpcs.ch.mu.Lock()
	DPrintf("ch [%016x]: GetPred got rpc lock", rpcs.ch.n.Hash)
	reply.N = rpcs.ch.predecessor
	rpcs.ch.mu.Unlock()
	return nil
}

// GetSuccArgs holds arguments for GetSucc.
type GetSuccArgs interface{}

// GetSuccReply holds reply to GetSucc.
type GetSuccReply struct {
	N Node
}

// GetSucc returns the Successor of the Chord instance on this server.
// LOCKS
func (rpcs *RPCServer) GetSucc(args *GetSuccArgs, reply *GetSuccReply) error {
	rpcs.checkInit()
	rpcs.mu.Lock()
	reply.N = rpcs.ch.ftable[0]
	rpcs.mu.Unlock()
	return nil
}

// FindClosestArgs holds arguments for FindClosestNode.
type FindClosestArgs struct {
	H UHash
}

// FindClosestReply holds reply to FindClosestReply.
type FindClosestReply struct {
	Done     bool
	N        Node
	ChFields ChordFields
}

// FindClosestNode finds the closest node to a hash from the Chord instance on this server.
func (rpcs *RPCServer) FindClosestNode(args *FindClosestArgs, reply *FindClosestReply) error {
	rpcs.checkInit()
	reply.Done = false

	DPrintf("ch [%s]: FindClosestNode (%016x)", rpcs.ch.n.String(), args.H)

	// TODO: Not happy about doing locking here
	rpcs.ch.mu.Lock()
	tempN := rpcs.ch.FindClosestNode(args.H)
	rpcs.ch.mu.Unlock()
	reply.N = tempN

	DPrintf("ch [%s]: FindClosestNode (%016x): secceeded: %s",
		rpcs.ch.n.String(), args.H, tempN.String())

	if rpcs.ch.n.Hash == reply.N.Hash {
		reply.Done = true
		reply.ChFields = *serializeChord(rpcs.ch)
	}
	return nil
}

// ChordFields holds values for building a chord instance.
// It is used to make passing around chord isntance information easier
type ChordFields struct {
	N           Node
	Predecessor Node
	FTable      []Node
}

func serializeChord(ch *Chord) *ChordFields {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	// Defensive copy of the underlying ftable
	nftable := make([]Node, len(ch.ftable))
	for i := 0; i < len(ch.ftable); i++ {
		nftable[i] = ch.ftable[i]
	}

	return &ChordFields{
		N:           ch.n,
		Predecessor: ch.predecessor,
		FTable:      nftable,
	}
}

func deserializeChord(chFields *ChordFields) *Chord {
	if chFields == nil {
		return nil
	}

	// Defensive copy of the underlying slice
	ftable := make([]Node, len(chFields.FTable))
	for i := 0; i < len(chFields.FTable); i++ {
		ftable[i] = chFields.FTable[i]
	}

	return &Chord{
		n:           chFields.N,
		predecessor: chFields.Predecessor,
		ftable:      ftable,
		isRunning:   true,
	}
}

// GetChordFieldsArgs is an empty interface
type GetChordFieldsArgs interface{}

// GetChordFieldsReply is a ChordFields type
type GetChordFieldsReply ChordFields

// GetChordFields ...
func (rpcs *RPCServer) GetChordFields(args *GetChordFieldsArgs, reply *GetChordFieldsReply) error {
	rpcs.checkInit()
	*reply = GetChordFieldsReply(*serializeChord(rpcs.ch))
	return nil
}

// ForwardLookupArgs holds arguments for FindClosestNode.
type ForwardLookupArgs struct {
	H        UHash
	RID      int
	Hops     int
	ChFields ChordFields
}

// ForwardLookupReply is an empty interface.
type ForwardLookupReply interface{}

// ForwardLookup finds the closest node to a hash from the Chord instance on this server.
func (rpcs *RPCServer) ForwardLookup(args *ForwardLookupArgs, reply *ForwardLookupReply) error {
	rpcs.checkInit()
	source := deserializeChord(&args.ChFields)
	return rpcs.ch.ForwardLookup(args.H, source, args.RID, args.Hops)
}

// ChordNotify calls notify on local chord instance.
func (rpcs *RPCServer) ChordNotify(args *Node, reply *struct{}) error {
	rpcs.checkInit()
	return rpcs.ch.Notify(*args)
}

// Ping used for testing
func (rpcs *RPCServer) Ping(args struct{}, reply *struct{}) error {
	rpcs.checkInit()
	return nil
}

// Return true if server is up. Return false otherwise.
func (rpcs *RPCServer) isRunning() bool {
	rpcs.mu.Lock()
	defer rpcs.mu.Unlock()
	return rpcs.running
}

// LookupResultArgs holds result of lookup
type LookupResultArgs struct {
	RID      int
	Hops     int
	ChFields ChordFields
}

// LookupResultReply is an empty interface.
type LookupResultReply interface{}

// ReceiveLookupResult receives the result of a recursive lookup
func (rpcs *RPCServer) ReceiveLookupResult(args *LookupResultArgs, reply *LookupResultReply) error {
	rpcs.checkInit()
	result := deserializeChord(&args.ChFields)
	res := LookupResult{args.RID, result, args.Hops, nil}
	return rpcs.ch.receiveLookUpResult(res)
}

// UpdateFtableArgs are arguments to UpdateFtable.
type UpdateFtableArgs struct {
	N Node
	I int
}

// UpdateFtable is RPC endpoint for chord.UpdateFtable().
func (rpcs *RPCServer) UpdateFtable(args *UpdateFtableArgs, reply *struct{}) error {
	rpcs.checkInit()
	return rpcs.ch.UpdateFtable(args.N, args.I)
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

// Wait blocks until server stops.
func (rpcs *RPCServer) Wait() error {
	if !rpcs.isRunning() {
		return nil
	}

	return <-rpcs.errChan
}

// End the server if it is running. Returns nil on success.
func (rpcs *RPCServer) End() error {
	if !rpcs.isRunning() {
		return nil
	}

	if err := rpcs.servListener.Close(); err != nil {
		return fmt.Errorf("error closing listener: %s", err)
	}

	return nil
}

// GetAddr returns the address the servers is listening on.
func (rpcs *RPCServer) GetAddr() net.Addr {
	return rpcs.servListener.Addr()
}
