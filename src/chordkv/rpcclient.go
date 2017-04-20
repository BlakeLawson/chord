// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package chordkv

import (
	"net/rpc"
)

// RemoteGet performs Get RPC on remote node.
func RemoteGet(n *Node, key string) (string, error) {
	client, err := rpc.DialHTTP("tcp", n.String())
	if err != nil {
		return "", err
	}

	args := &KVGetArgs{key}
	var reply KVGetReply
	err = client.Call("RPCServer.KVGet", args, &reply)
	if err != nil {
		return "", err
	}
	return reply.Val, nil
}

// RemotePut performs Put RPC on remote node.
func RemotePut(n *Node, key string, val string) error {
	client, err := rpc.DialHTTP("tcp", n.String())
	if err != nil {
		return err
	}

	args := &KVPutArgs{key, val}
	var reply KVPutReply
	err = client.Call("RPCServer.KVPut", args, &reply)
	return err
}

// RemoteLookup performs Lookup RPC on remote node.
func RemoteLookup(n *Node, h UHash) (*Chord, error) {
	client, err := rpc.DialHTTP("tcp", n.String())
	if err != nil {
		return nil, err
	}

	args := &ChordLookupArgs{h}
	var reply ChordLookupReply
	err = client.Call("RPCServer.ChordLookup", args, &reply)
	if err != nil {
		return nil, err
	}

	ch := &Chord{}
	ch.n = reply.N
	ch.predecessor = reply.Predecessor
	ch.ftable = reply.FTable
	ch.slist = reply.SList
	return ch, nil
}

// RemoteGetPred returns the predecessor of the specified node.
func RemoteGetPred(n *Node) (*Node, error) {
	client, err := rpc.DialHTTP("tcp", n.String())
	if err != nil {
		return nil, err
	}

	args := new(GetPredArgs)
	var reply GetPredReply
	err = client.Call("RPCServer.GetPred", args, &reply)
	if err != nil {
		return nil, err
	}
	return &reply.N, nil
}

// RemoteFindClosestNode find the closest node from n to hash identifier h
func RemoteFindClosestNode(h UHash, ch *Chord) (*Chord, error) {
	client, err := rpc.DialHTTP("tcp", ch.n.String())
	if err != nil {
		return nil, err
	}

	args := &FindClosestArgs{h}
	var reply FindClosestReply
	err = client.Call("RPCServer.FindClosestNode", args, &reply)
	if err != nil {
		return nil, err
	}

	resCh := &Chord{}
	resCh.n = reply.N
	resCh.predecessor = reply.Predecessor
	resCh.ftable = reply.FTable
	resCh.slist = reply.SList
	return resCh, nil
}

// RemoteForwardLookup forwards source's lookup on h to dest
func RemoteForwardLookup(h UHash, source *Chord, rID int, dest *Chord) error {
	client, err := rpc.DialHTTP("tcp", dest.n.String())
	if err != nil {
		return err
	}

	args := &ForwardLookupArgs{
		h: h,
		sourceFields: ChordFields{
			N:           source.n,
			Predecessor: source.predecessor,
			FTable:      source.ftable,
			SList:       source.slist},
		rID: rID}

	var reply FindClosestReply
	err = client.Call("RPCServer.ForwardLookup", args, &reply)
	if err != nil {
		return err
	}
	return nil
}

// RemoteSendLookupResult sends the lookup result from the result chord node
// to the source of the lookup with the request ID rID
func RemoteSendLookupResult(source *Chord, rID int, result *Chord) error {
	return nil
}

// RemoteGetChordFromNode returns a chord instance representing the state at node's
// chord instance. This is to deal with the fact that many methods return a chord type
// but the fingertable stores node information
func RemoteGetChordFromNode(n *Node) (*Chord, error) {
	return nil, nil
}
