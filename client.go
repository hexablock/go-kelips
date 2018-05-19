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
func (c *Client) getpeer() string {
	i := rand.Int() % len(c.peers)
	return c.peers[i]
}

// LookupGroupNodes requests group nodes for a key from a peer
func (c *Client) LookupGroupNodes(key []byte) ([]*hexatype.Node, error) {
	host := c.getpeer()
	return c.trans.LookupGroupNodes(host, key)
}

// LookupNodes request nodes for key returning atleast min number of nodes
func (c *Client) LookupNodes(key []byte, min int) ([]*hexatype.Node, error) {
	host := c.getpeer()
	return c.trans.LookupNodes(host, key, min)
}

// Lookup returns nodes holding the key
func (c *Client) Lookup(key []byte) ([]*hexatype.Node, error) {
	host := c.getpeer()
	return c.trans.Lookup(host, key)
}

// Insert sends an insert request for key-tuple mapping to a peer
func (c *Client) Insert(key []byte, tuple TupleHost) error {
	host := c.getpeer()
	return c.trans.Insert(host, key, tuple, true)
}

// Delete sends a delete request to delete a key
func (c *Client) Delete(key []byte, tuple TupleHost) error {
	host := c.getpeer()
	return c.trans.Delete(host, key, tuple, true)
}
