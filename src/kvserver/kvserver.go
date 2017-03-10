// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)
package kvserver

import (
	"chord"
	"sync"
)

// KVServer represents node in distributed key value storage system.
type KVServer struct {
	mu    sync.Mutex
	ch    *chord.Chord
	state map[string]string
}

// Get returns value associated with given key. Returns empty string if key
// not present.
func (kvs *KVServer) Get(key string) string {
	return ""
}

// Put inserts key-value pair into server state. Creates new entry if not
// present. Otherwise overwrites existing value.
func (kvs *KVServer) Put(key string, val string) {

}

// Make a new KVServer instance.
func Make(ch *chord.Chord) *KVServer {
	return &KVServer{
		mu:    sync.Mutex{},
		ch:    ch,
		state: make(map[string]string)}
}
