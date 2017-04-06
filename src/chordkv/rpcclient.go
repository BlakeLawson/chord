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
func RemoteLookup(n *Node, key string) (*Node, error) {
	client, err := rpc.DialHTTP("tcp", n.String())
	if err != nil {
		return nil, err
	}

	args := &ChordLookupArgs{key}
	var reply ChordLookupReply
	err = client.Call("RPCServer.ChordLookup", args, &reply)
	if err != nil {
		return nil, err
	}

	return MakeNode(reply.Addr, reply.Port), nil
}
