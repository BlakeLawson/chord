// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)
package kvclient

import (
	"chord"
	"kvserver"
)

// Right now, this implementation assumes that all chord and server instances
// are running on the same machine for simplicity. At some point, will need to
// rewrite so it goes over the network.

type KVClient struct {
}

func (kvc *KVClient) Get(key string) (string, error) {

}

func (kvc *KVClient) Put(key string, val string) error {

}

func Make() *KVClient {

}
