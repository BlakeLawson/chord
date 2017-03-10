// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package kvclient

import (
	"chord"
)

// KVClient represents client for distributed kv store.
type KVClient struct {
	ch *chord.Chord
}

// Get returns value associated with given key. Return nil on success.
func (kvc *KVClient) Get(key string) (string, error) {
	node, err := ch.Lookup(key)
	//TODO get value from node.

	return "", err
}

// Put inserts key value pair into distributed kv store. Returns nil on
// success.
func (kvc *KVClient) Put(key string, val string) error {
	node, err := ch.Lookup(key)
	//TODO put value from node.
	return nil
}

// Make a new KVClient instance.
func Make(ch *chord.Chord) *KVClient {
	return &KVClient{ch}
}
