package kelips

import (
	"log"
	"time"

	msgpack "gopkg.in/vmihailenco/msgpack.v2"

	"github.com/hashicorp/serf/serf"
)

// Transport needed by kelips
type Transport interface {
	Ping(node *Node) time.Duration
	// Insert a key-host association
	Insert(key []byte, host *Host) error
	// Lookup a key on the optional hosts
	Lookup(key []byte, hosts ...string) ([]Node, error)
}

// SerfTransport implements a Transport interface using serf for gossip
type SerfTransport struct {
	serf         *serf.Serf
	queryTimeout time.Duration
}

// NewSerfTransport inits a new serf backed gossip transport with a default
// query timeout of 3 seconds
func NewSerfTransport(srf *serf.Serf) *SerfTransport {
	return &SerfTransport{serf: srf, queryTimeout: 3 * time.Second}
}

// Ping gets the coordinates of the remote and local node and retursn the
// distance ie responcse time.  If the supplied node is the node itself or an
// error occurs 0 is returned
func (trans *SerfTransport) Ping(node *Node) time.Duration {
	if node.Name == trans.serf.LocalMember().Name {
		return 0
	}

	remote, ok := trans.serf.GetCachedCoordinate(node.String())
	if !ok {
		return 0
	}

	self, err := trans.serf.GetCoordinate()
	if err != nil {
		log.Println("[ERROR] Failed to get self coordinates:", err)
		return 0
	}

	return self.DistanceTo(remote)
}

// Lookup submits a lookup request to the optionally supplied nodes. If no hosts
// are specified the query is sent out to all nodes
func (trans *SerfTransport) Lookup(key []byte, hosts ...string) ([]Node, error) {
	var (
		params  = &serf.QueryParam{FilterNodes: hosts, Timeout: trans.queryTimeout}
		promise *serf.QueryResponse
		nodes   []Node
		err     error
	)

	if promise, err = trans.serf.Query("lookup", key, params); err == nil {
		resp := <-promise.ResponseCh()
		//resp.From
		err = msgpack.Unmarshal(resp.Payload, &nodes)
	}

	return nodes, err
}

// Insert broadcasts an insert to the network.  The format of the payload is
// key, 16-byte address, 2 byte port
func (trans *SerfTransport) Insert(key []byte, host *Host) error {
	hb := host.Bytes()
	// ipv4 - append 12 more to get 18bytes
	if len(hb) == 6 {
		hb = append(make([]byte, 12), hb...)
	}

	return trans.serf.UserEvent("insert", append(key, hb...), true)
}
