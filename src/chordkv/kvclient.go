// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package chordkv

// KVClient represents client for distributed kv store.
type KVClient struct {
	ch *Chord
}

// Get returns value associated with given key. Return nil on success.
func (kvc *KVClient) Get(key string) (string, error) {
	node, err := kvc.ch.Lookup(key)
	if err != nil {
		return "", err
	}

	result, err := RemoteGet(node, key)
	return result, err
}

// Put inserts key value pair into distributed kv store. Returns nil on
// success.
func (kvc *KVClient) Put(key string, val string) error {
	node, err := kvc.ch.Lookup(key)
	if err != nil {
		return err
	}

	err = RemotePut(node, key, val)
	return err
}

// Make a new KVClient instance.
func MakeKVClient(ch *Chord) *KVClient {
	return &KVClient{ch}
}
