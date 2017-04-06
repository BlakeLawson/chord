// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package chordkv

import (
	"sync"
)

// KVServer represents node in distributed key value storage system.
type KVServer struct {
	mu    sync.Mutex
	ch    *Chord
	state map[string]string
}

// Get returns value associated with given key. Returns empty string if key
// not present.
func (kvs *KVServer) Get(key string) string {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	return kvs.state[key]
}

// Put inserts key-value pair into server state. Creates new entry if not
// present. Otherwise overwrites existing value.
func (kvs *KVServer) Put(key string, val string) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	kvs.state[key] = val
}

// Make a new KVServer instance.
func MakeKVServer(ch *Chord) *KVServer {
	return &KVServer{
		mu:    sync.Mutex{},
		ch:    ch,
		state: make(map[string]string)}
}
