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
func RemoteFindClosestNode(h UHash, n *Node) (*Node, error) {
	return nil, nil
}

// RemoteForwardLookup forwards source's lookup on h to dest
func RemoteForwardLookup(h UHash, source *Node, rID int, dest *Node) error {
	return nil
}

// RemoteSendLookupResult sends the lookup result from the result node
// to the source of the lookup with the request ID rID
func RemoteSendLookupResult(source *Node, rID int, result *Node) error {
	return nil
}
