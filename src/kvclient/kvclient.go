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
	node, err := kvc.ch.Lookup(key)
	if err != nil {
		return "", err
	}
	//TODO get value from node.
	result, err := node.RemoteGet(key)
	return result, err
}

// Put inserts key value pair into distributed kv store. Returns nil on
// success.
func (kvc *KVClient) Put(key string, val string) error {
	node, err := kvc.ch.Lookup(key)
	if err != nil {
		return err
	}
	//TODO put value from node.
	err = node.RemotePut(key, val)
	return err
}

// Make a new KVClient instance.
func Make(ch *chord.Chord) *KVClient {
	return &KVClient{ch}
}
