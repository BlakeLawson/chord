// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package chordkv

// KVClient represents client for distributed kv store.
type KVClient struct {
	ch *Chord
}

// Get returns value associated with given key. Return nil on success.
func (kvc *KVClient) Get(key string) (string, error) {
	chNode, err := kvc.ch.Lookup(Hash(key))
	if err != nil {
		return "", err
	}

	result, err := RemoteGet(chNode.n, key)
	return result, err
}

// Put inserts key value pair into distributed kv store. Returns nil on
// success.
func (kvc *KVClient) Put(key string, val string) error {
	chNode, err := kvc.ch.Lookup(Hash(key))
	if err != nil {
		return err
	}

	err = RemotePut(chNode.n, key, val)
	return err
}

// MakeKVClient creates a new KVClient instance.
func MakeKVClient(ch *Chord) *KVClient {
	return &KVClient{ch}
}
