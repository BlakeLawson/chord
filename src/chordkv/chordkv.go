// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package chordkv

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
)

// externalIP looks up this machine's IP address. This method from
// http://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go
func externalIP() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return "", err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue // not an ipv4 address
			}
			return ip.String(), nil
		}
	}
	return "", errors.New("are you connected to the network?")
}

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
func MakeChordKV(existingNode *Node) (*ChordKV, error) {
	return MakeChordKVDbg(existingNode, false)
}

// MakeChordKVDbg is the same as MakeChordKV, but allows user to specify
// whether to use localhost instead of actual IP addresses. Used for testing.
func MakeChordKVDbg(existingNode *Node, useLocalhost bool) (*ChordKV, error) {
	// Placeholders for RPCServer initialization
	ch := &Chord{}
	kvs := &KVServer{}

	// Get the IP at the start because there is no point continuing if this fails.
	DPrintf("MakeChordKVDbg: getting externalIP")
	ip, err := externalIP()
	if err != nil {
		return nil, fmt.Errorf("Could not find IP: %s", err)
	}

	// Initialize RPCServer with port 0 so OS picks the port.
	var addr string
	if useLocalhost {
		addr = "127.0.0.1:0"
	} else {
		addr = ":0"
	}

	DPrintf("MakeChordKVDbg: calling startRPC")
	rpcs, err := startRPC(ch, kvs, addr)
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
