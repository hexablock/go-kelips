package kelips

import (
	"fmt"
	"math/rand"

	"github.com/hexablock/hexatype"
)

type Client struct {
	trans *UDPTransport

	peers []string
}

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

func (client *Client) getpeer() string {
	i := rand.Int() % len(client.peers)
	return client.peers[i]
}

func (client *Client) LookupGroupNodes(key []byte) ([]*hexatype.Node, error) {
	host := client.getpeer()
	return client.trans.LookupGroupNodes(host, key)
}
func (client *Client) Lookup(key []byte) ([]*hexatype.Node, error) {
	host := client.getpeer()
	return client.trans.Lookup(host, key)
}
func (client *Client) Insert(key []byte, tuple TupleHost) error {
	host := client.getpeer()
	return client.trans.Insert(host, key, tuple, true)
}
func (client *Client) Delete(key []byte) error {
	host := client.getpeer()
	return client.trans.Delete(host, key, true)
}
