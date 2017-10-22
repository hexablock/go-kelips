package kelips

import (
	"encoding/binary"
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

// NewHostFromBytes returns a new Host struct from the bytes.  The last 2 bytes
// are the port and the remainder is the address
func NewHostFromBytes(b []byte) *Host {
	host := &Host{}
	m := len(b) - 2
	host.Port = binary.BigEndian.Uint16(b[m:])
	host.Addr = net.IP(b[:m])

	return host
}

// NewHost creates a new host with the address and port
func NewHost(addr string, port uint16) *Host {
	return &Host{Addr: net.ParseIP(addr), Port: port}
}

// Bytes returns the address bytes followed by 2 byte port
func (host *Host) Bytes() []byte {
	port := make([]byte, 2)
	binary.BigEndian.PutUint16(port, host.Port)
	return append([]byte(host.Addr), port...)
}

func (host *Host) String() string {
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
	// Number of heartbeats received
	Heartbeats int

	// Round-trip-time to the node
	RTT time.Duration

	// Last time contact was made
	LastSeen time.Time
}

func (n *Node) init(f func() hash.Hash) {
	h := f()
	// Generate id from host:port
	h.Write([]byte(n.String()))
	sh := h.Sum(nil)
	n.ID = sh[:]
}

// MarshalJSON is a custom json marshal for a node to handle id's and times
func (n Node) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		ID         string
		Name       string
		Tags       map[string]string
		Host       string
		Heartbeats int
		RTT        string
		LastSeen   time.Time
	}{
		hex.EncodeToString(n.ID),
		n.Name,
		n.Tags,
		n.String(),
		n.Heartbeats,
		n.RTT.String(),
		n.LastSeen,
	})
}
