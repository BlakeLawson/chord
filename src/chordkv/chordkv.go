// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package chordkv

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// ChordKV contains everything everything necessary to use the chord lookup
// service with a distributed key value store.
type ChordKV struct {
	Ch     *Chord
	Kvs    *KVServer
	Rpcs   *RPCServer
	Client *KVClient
}

// MakeChordKV returns fully initialized chord instance and associated rpc
// server and kv server. Takes existingNode to initialize with an existing
// chord ring. If existingNode is nil, creates new chord ring.
func MakeChordKV(ip string, existingNode *Node) (*ChordKV, error) {
	// Placeholders for RPCServer initialization
	ch := &Chord{}
	kvs := &KVServer{}

	// Validate IP
	if ok := net.ParseIP(ip); ok == nil {
		return nil, fmt.Errorf("IP addr invalid: %s", ip)
	}

	DPrintf("MakeChordKVDbg: calling startRPC")
	// Ignore given IP for initialization because AWS uses annoying configuration
	// with different public and private IP addresses.
	rpcs, err := startRPC(ch, kvs, ":0")
	if err != nil {
		return nil, fmt.Errorf("RPCServer initialilzation failed: %s", err)
	}

	// Ensure that rpcs does not serve any requests until ch is initialized.
	rpcs.initBarrier.Lock()
	defer rpcs.initBarrier.Unlock()

	rpcAddr := rpcs.GetAddr().String()
	_, portString, err := net.SplitHostPort(rpcAddr)
	if err != nil {
		rpcs.End()
		return nil, fmt.Errorf("Parsing %s failed: %s", rpcAddr, err)
	}
	p, err := strconv.Atoi(portString)
	if err != nil {
		rpcs.End()
		return nil, fmt.Errorf("Failed to parse %s: %s", portString, err)
	}

	n := MakeNode(net.ParseIP(ip), p)
	DPrintf("MakeChordKVDbg: calling MakeChord %s(%016x)", n.String(), n.Hash)
	if existingNode == nil {
		ch, err = MakeChord(n, Node{}, true)
	} else {
		ch, err = MakeChord(n, *existingNode, false)
	}
	if err != nil {
		rpcs.End()
		return nil, fmt.Errorf("Chord initialization failed: %s", err)
	}

	kvs = MakeKVServer(ch)
	rpcs.ch = ch
	rpcs.kv = kvs

	return &ChordKV{ch, kvs, rpcs, MakeKVClient(ch)}, nil
}

// Kill disables chkv.
func (chkv *ChordKV) Kill() error {
	chkv.Ch.Kill()
	if err := chkv.Rpcs.End(); err != nil {
		// Suppress expected error message.
		if !strings.Contains(err.Error(), "closed network connection") {
			return err
		}
	}
	return nil
}
