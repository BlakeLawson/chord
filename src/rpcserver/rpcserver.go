// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)
package rpcserver

import (
	"chord"
	"kvserver"
	"sync"
)

// This package expects to be called once from
var ch *chord.Chord
var kv *kvserver.KVServer

// Start RPCServer listening on given port. Does not return until error or
// program ends. It is an error to call Start more than once.
func Start(ch *chord.Chord, kv *kvserver.KVServer, port int) error {
}
