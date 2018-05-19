package kelipspb

import (
	"hash"
)

// NewNode returns a new node with the given ip and port
func NewNode(addr string, port int) *Node {
	return &Node{Address: NewAddress(addr, port)}
}

// HashID returns the hash the node address
func (node *Node) HashID(h hash.Hash) []byte {
	h.Write(node.Address)
	sh := h.Sum(nil)
	return sh
}
