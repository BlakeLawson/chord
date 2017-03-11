// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)
package util

import (
	"fmt"
	"net"
	"net/rpc"
	"rpcserver"
)

// Node in Chord ring.
type Node struct {
	Addr net.IP
	Port int
	Hash UHash
}

// Initialize a node. Return nil to indicate error.
func MakeNode(addr net.IP, port int) *Node {
	if addr == nil || port <= 0 {
		return nil
	}

	n := &Node{addr, port, 0}
	n.Hash = Hash(n.String())
	return n
}

func (n *Node) String() string {
	return fmt.Sprintf("%s:%d", n.Addr.String(), n.Port)
}

// RemoteGet performs Get RPC on remote node.
func (n *Node) RemoteGet(key string) (string, error) {
	client, err := rpc.DialHTTP("tcp", n.String())
	if err != nil {
		return "", err
	}

	args := &rpcserver.KVGetArgs{key}
	var reply rpcserver.KVGetReply
	err = client.Call("RPCServer.KVGet", args, &reply)
	if err != nil {
		return "", err
	}
	return reply.Val, nil
}

// RemotePut performs Put RPC on remote node.
func (n *Node) RemotePut(key string, val string) error {
	client, err := rpc.DialHTTP("tcp", n.String())
	if err != nil {
		return err
	}

	args := &rpcserver.KVPutArgs{key, val}
	var reply rpcserver.KVPutReply
	err = client.Call("RPCServer.KVPut", args, &reply)
	return err
}

// RemoteLookup performs Lookup RPC on remote node.
func (n *Node) RemoteLookup(key string) (*Node, error) {
	client, err := rpc.DialHTTP("tcp", n.String())
	if err != nil {
		return nil, err
	}

	args := &rpcserver.ChordLookupArgs{key}
	var reply ChordLookupReply
	err = client.Call("RPCServer.ChordLookup", args, &reply)
	if err != nil {
		return nil, err
	}

	return MakeNode(reply.Addr, reply.Port)
}
