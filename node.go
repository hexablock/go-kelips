package kelips

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"net"
	"time"
)

// Host contains host address and port information
type Host struct {
	Addr net.IP
	Port uint16
}

// NewHost creates a new host with the address and port
func NewHost(addr string, port uint16) *Host {
	return &Host{Addr: net.ParseIP(addr), Port: port}
}

// Hostname returns the host:port string
func (host *Host) Hostname() string {
	return host.Addr.String() + ":" + fmt.Sprintf("%d", host.Port)
}

// Node contains information about a node in the cluster.  This is materialized based
// on gossip events.
type Node struct {
	*Host
	// Hash id of the node
	ID   []byte
	Name string
	Tags map[string]string
	// Run status of the node
	Status int

	// Number of heartbeats received
	Heartbeats int

	// Round-trip-time to the node
	RTT time.Duration

	// Last time contact was made
	LastSeen time.Time
}

func (n *Node) String() string {
	return n.Hostname()
}

func (n *Node) init(f func() hash.Hash) {
	h := f()
	// Generate id from host:port
	h.Write([]byte(n.Hostname()))
	sh := h.Sum(nil)
	n.ID = sh[:]
}

// MarshalJSON is a custom json marshal for a node to handle id's and times
func (n Node) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		ID         string
		Name       string
		Tags       map[string]string
		Status     int
		Host       string
		Heartbeats int
		RTT        string
		LastSeen   time.Time
	}{
		hex.EncodeToString(n.ID),
		n.Name,
		n.Tags,
		n.Status,
		n.Hostname(),
		n.Heartbeats,
		n.RTT.String(),
		n.LastSeen,
	})
}
