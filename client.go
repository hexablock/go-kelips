package kelips

import (
	"fmt"
	"math/rand"

	"github.com/hexablock/hexatype"
)

// Client implements a kelips client
type Client struct {
	trans *UDPTransport
	// existing peers
	peers []string
}

// NewClient inits a new client using exising peers
func NewClient(peers ...string) (*Client, error) {
	if len(peers) == 0 {
		return nil, fmt.Errorf("peers required")
	}

	client := &Client{
		peers: peers,
		trans: NewUDPTransport(nil),
	}
	return client, nil
}

// randomly get a peer
func (client *Client) getpeer() string {
	i := rand.Int() % len(client.peers)
	return client.peers[i]
}

// LookupGroupNodes requests group nodes for a key from a peer
func (client *Client) LookupGroupNodes(key []byte) ([]*hexatype.Node, error) {
	host := client.getpeer()
	return client.trans.LookupGroupNodes(host, key)
}

// Lookup returns nodes holding the key
func (client *Client) Lookup(key []byte) ([]*hexatype.Node, error) {
	host := client.getpeer()
	return client.trans.Lookup(host, key)
}

// Insert sends an insert request for key-tuple mapping to a peer
func (client *Client) Insert(key []byte, tuple TupleHost) error {
	host := client.getpeer()
	return client.trans.Insert(host, key, tuple, true)
}

// Delete sends a delete request to delete a key
func (client *Client) Delete(key []byte) error {
	host := client.getpeer()
	return client.trans.Delete(host, key, true)
}
