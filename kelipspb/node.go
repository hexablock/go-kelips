package kelipspb

import (
	"encoding/hex"
	"encoding/json"
	"hash"
	"time"

	vivaldi "github.com/hexablock/vivaldi"
)

// NewNode returns a new node with the given ip and port
func NewNode(addr string, port int) *Node {
	return &Node{Address: newAddress(addr, port)}
}

// HashID returns the hash the node address
func (node *Node) HashID(h hash.Hash) []byte {
	h.Write(node.Address)
	sh := h.Sum(nil)
	return sh
}

// MarshalJSON marshals the node to human readable json
func (node *Node) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		ID          string
		Address     string
		LastSeen    time.Time
		Heartbeats  uint32              `json:",omitempty"`
		Latency     time.Duration       `json:",omitempty"`
		Meta        map[string]string   `json:",omitempty"`
		Coordinates *vivaldi.Coordinate `json:",omitempty"`
	}{
		hex.EncodeToString(node.ID),
		node.Address.String(),
		time.Unix(0, node.LastSeen),
		node.Heartbeats,
		node.Latency,
		node.Meta,
		node.Coordinates,
	})
}
