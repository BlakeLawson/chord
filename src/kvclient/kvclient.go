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
	return "", nil
}

// Put inserts key value pair into distributed kv store. Returns nil on
// success.
func (kvc *KVClient) Put(key string, val string) error {
	return nil
}

// Make a new KVClient instance.
func Make(ch *chord.Chord) *KVClient {
	return &KVClient{ch}
}
