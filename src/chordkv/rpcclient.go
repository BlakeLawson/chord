// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package chordkv

import (
	"net"
	"net/rpc"
	"time"
)

func (n *Node) openConn() (*rpc.Client, error) {
	conn, err := net.DialTimeout("tcp", n.String(), 5*time.Second)
	if err != nil {
		return nil, err
	}
	return rpc.NewClient(conn), nil
}

// RemoteGet performs Get RPC on remote node.
func (n *Node) RemoteGet(key string) (string, error) {
	client, err := n.openConn()
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
func (n *Node) RemotePut(key string, val string) error {
	client, err := n.openConn()
	if err != nil {
		return err
	}

	args := &KVPutArgs{key, val}
	var reply KVPutReply
	err = client.Call("RPCServer.KVPut", args, &reply)
	return err
}

// RemoteLookup performs Lookup RPC on remote node.
func (n *Node) RemoteLookup(h UHash) (*Chord, error) {
	client, err := n.openConn()
	if err != nil {
		return nil, err
	}

	args := &ChordLookupArgs{h}
	var reply ChordLookupReply
	err = client.Call("RPCServer.ChordLookup", args, &reply)
	if err != nil {
		return nil, err
	}

	return deserializeChord(&reply.ChFields), nil
}

// RemoteGetPred returns the predecessor of the specified node.
func (n *Node) RemoteGetPred() (*Node, error) {
	client, err := n.openConn()
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

// RemoteFindClosestNode find the closest node from n to hash identifier h.
// Returns the closest known Node and the Chord instance for that node if the
// Node is the actual Node that is responsible for h. If the returned Node is
// not responsible for h then the Chord return is nil.
func (n *Node) RemoteFindClosestNode(h UHash) (*Node, *Chord, error) {
	client, err := n.openConn()
	if err != nil {
		return nil, nil, err
	}

	args := &FindClosestArgs{h}
	var reply FindClosestReply
	err = client.Call("RPCServer.FindClosestNode", args, &reply)
	if err != nil {
		return nil, nil, err
	}

	return &reply.N, deserializeChord(reply.ChFields), nil
}

// RemoteForwardLookup forwards source's lookup on h to dest
func (n *Node) RemoteForwardLookup(h UHash, source *Chord, rID int) error {
	client, err := n.openConn()
	if err != nil {
		return err
	}

	args := &ForwardLookupArgs{
		H:        h,
		RID:      rID,
		ChFields: *serializeChord(source)}

	var reply FindClosestReply
	err = client.Call("RPCServer.ForwardLookup", args, &reply)
	if err != nil {
		return err
	}
	return nil
}

// RemoteSendLookupResult sends the lookup result from the result chord node
// to the source of the lookup with the request ID rID
func (n *Node) RemoteSendLookupResult(rID int, result *Chord) error {
	return nil
}

// RemoteGetChordFromNode returns a chord instance representing the state at node's
// chord instance. This is to deal with the fact that many methods return a chord type
// but the fingertable stores node information
func (n *Node) RemoteGetChordFromNode() (*Chord, error) {
	return nil, nil
}

// RemotePing used for testing RPC functionality.
func (n *Node) RemotePing() error {
	client, err := n.openConn()
	if err != nil {
		return err
	}

	var reply struct{}
	return client.Call("RPCServer.Ping", reply, &reply)
}
