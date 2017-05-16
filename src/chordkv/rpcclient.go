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
	return client.Call("RPCServer.KVPut", args, &reply)
}

// RemoteKVSize performs Size RPC on KVServer of remote node
func (n *Node) RemoteKVSize() (int, error) {
	client, err := n.openConn()
	if err != nil {
		return 0, err
	}
	defer client.Close()

	var args KVSizeArgs
	reply := KVSizeReply{}
	err = client.Call("RPCServer.KVSize", &args, &reply)
	if err != nil {
		return 0, err
	}
	return reply.Size, nil
}

// RemoteLookup performs Lookup RPC on remote node.
func (n *Node) RemoteLookup(h UHash) (*Chord, *LookupInfo, error) {
	DPrintf("Remote lookup %s -> (%016x)", n.String(), h)
	client, err := n.openConn()
	if err != nil {
		return nil, nil, err
	}
	defer client.Close()

	args := &ChordLookupArgs{h}
	var reply ChordLookupReply
	err = client.Call("RPCServer.ChordLookup", args, &reply)
	if err != nil {
		return nil, nil, err
	}

	return deserializeChord(&reply.ChFields), &(reply.Info), nil
}

// RemoteGetPred returns the predecessor of the specified node.
func (n *Node) RemoteGetPred() (Node, error) {
	client, err := n.openConn()
	if err != nil {
		return Node{}, err
	}
	defer client.Close()

	args := new(GetPredArgs)
	var reply GetPredReply
	err = client.Call("RPCServer.GetPred", args, &reply)
	if err != nil {
		return Node{}, err
	}
	return reply.N, nil
}

// RemoteGetSucc returns the successor of the specified node.
// THE SERVER LOCKS THE CHORD MUTEX TO ACCESS THE SUCCESSOR
func (n *Node) RemoteGetSucc() (*Node, error) {
	client, err := n.openConn()
	if err != nil {
		return nil, err
	}
	defer client.Close()

	args := new(GetSuccArgs)
	var reply GetSuccReply
	err = client.Call("RPCServer.GetSucc", args, &reply)
	if err != nil {
		return nil, err
	}
	return &reply.N, nil
}

// RemoteFindClosestNode find the closest node from n to hash identifier h.
// Returns the closest known Node and the Chord instance for that node if the
// Node is the actual Node that is responsible for h. If the returned Node is
// not responsible for h then the Chord return is nil.
func (n *Node) RemoteFindClosestNode(h UHash) (Node, Chord, error) {
	DPrintf("ch [%s]: RemoteFindClosestNode (%016x): opening connection", n.String(), h)
	client, err := n.openConn()
	if err != nil {
		return Node{}, Chord{}, err
	}
	defer client.Close()

	args := &FindClosestArgs{h}
	var reply FindClosestReply
	DPrintf("ch [%s]: RemoteFindClosestNode (%016x): making RPC", n.String(), h)
	err = client.Call("RPCServer.FindClosestNode", args, &reply)
	if err != nil {
		DPrintf("ch [%s]: RemoteFindClosest (%016x): RPC failed: %s", n.String(), h, err)
		return Node{}, Chord{}, err
	}

	DPrintf("ch [%s]: RemoteFindClosest (%016x): RPC succeeded: "+
		"{Done:%v, N:%s, Ch:%s}", n.String(), h, reply.Done,
		reply.N.String(), reply.ChFields.N.String())

	if reply.Done {
		return reply.N, *deserializeChord(&reply.ChFields), nil
	}
	return reply.N, Chord{}, nil
}

// RemoteForwardLookup forwards source's lookup on h to dest
func (n *Node) RemoteForwardLookup(h UHash, source *Chord, rID, hops int) error {
	client, err := n.openConn()
	if err != nil {
		return err
	}
	defer client.Close()

	args := &ForwardLookupArgs{
		H:        h,
		RID:      rID,
		Hops:     hops,
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
func (n *Node) RemoteSendLookupResult(rID, hops int, result *Chord) error {
	client, err := n.openConn()
	if err != nil {
		return err
	}
	defer client.Close()

	args := &LookupResultArgs{
		RID:      rID,
		Hops:     hops,
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
// Times out after pingTimeout seconds
func (n *Node) RemotePing() error {
	client, err := n.openConn()
	if err != nil {
		return err
	}
	defer client.Close()

	var args, reply struct{}
	return client.Call("RPCServer.Ping", &args, &reply)
}

// RemoteNotify calls Notify on n.
func (n *Node) RemoteNotify(pred Node) error {
	client, err := n.openConn()
	if err != nil {
		return err
	}
	defer client.Close()

	var reply struct{}
	return client.Call("RPCServer.ChordNotify", pred, &reply)
}

// RemoteUpdateFtable calls UpdateFtable on n.
func (n *Node) RemoteUpdateFtable(m Node, i int) error {
	client, err := n.openConn()
	if err != nil {
		return err
	}
	defer client.Close()

	var reply struct{}
	return client.Call("RPCServer.UpdateFtable", &UpdateFtableArgs{m, i}, &reply)
}
