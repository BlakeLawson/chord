// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package chordkv

import (
	"fmt"
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
	defer client.Close()

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
	defer client.Close()

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
	defer client.Close()

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
	defer client.Close()

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
	defer client.Close()

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
	defer client.Close()

	args := &ForwardLookupArgs{
		H:        h,
		RID:      rID,
		ChFields: *serializeChord(source)}

	var reply ForwardLookupReply
	err = client.Call("RPCServer.ForwardLookup", args, &reply)
	if err != nil {
		return err
	}
	return nil
}

// RemoteSendLookupResult sends the lookup result from the result chord node
// to the source of the lookup with the request ID rID
func (n *Node) RemoteSendLookupResult(rID int, result *Chord) error {
	client, err := n.openConn()
	if err != nil {
		return err
	}
	defer client.Close()

	args := &LookupResultArgs{
		RID:      rID,
		ChFields: *serializeChord(result)}

	var reply LookupResultReply
	err = client.Call("RPCServer.ReceiveLookupResult", args, &reply)

	if err != nil {
		return err
	}
	return nil
}

// RemoteGetChordFromNode returns a chord instance representing the state at node's
// chord instance. This is to deal with the fact that many methods return a chord type
// but the fingertable stores node information
func (n *Node) RemoteGetChordFromNode() (*Chord, error) {
	client, err := n.openConn()
	if err != nil {
		return nil, err
	}
	defer client.Close()

	var args GetChordFieldsArgs
	var reply *GetChordFieldsReply
	err = client.Call("RPCServer.GetChordFields", &args, reply)
	if err != nil {
		return nil, err
	}
	temp := ChordFields(*reply)
	ch := deserializeChord(&temp)
	if ch == nil {
		return nil, fmt.Errorf("ChordFields reply empty %+v", ch)
	}
	return ch, nil
}

// RemotePing used for testing RPC functionality.
func (n *Node) RemotePing() error {
	client, err := n.openConn()
	if err != nil {
		return err
	}
	defer client.Close()

	var reply struct{}
	return client.Call("RPCServer.Ping", reply, &reply)
}

// RemoteNotify calls Notify on n.
func (n *Node) RemoteNotify(pred *Node) error {
	client, err := n.openConn()
	if err != nil {
		return err
	}
	defer client.Close()

	var reply struct{}
	return client.Call("RPCServer.ChordNotify", pred, &reply)
}

// RemoteUpdateFtable calls UpdateFtable on n.
func (n *Node) RemoteUpdateFtable(m *Node, i int) error {
	client, err := n.openConn()
	if err != nil {
		return err
	}
	defer client.Close()

	var reply struct{}
	return client.Call("RPCServer.UpdateFtable", &UpdateFtableArgs{m, i}, &reply)
}
