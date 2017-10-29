package kelips

import (
	"fmt"
	"log"
	"net"
	"time"

	msgpack "gopkg.in/vmihailenco/msgpack.v2"

	"github.com/hashicorp/serf/serf"
)

const (
	// lookup request
	queryLookup = "lookup"
	// insert was made
	userInsert = "insert"
)

// AffinityGroupRPC implements RPC's used by the network transport to serve
// requests. This is the local afffinity group for the node
type AffinityGroupRPC interface {
	AddNode(node *Node) error
	RemoveNode(host string) error
	AddTuple(key []byte, host *Host) bool
	GetTuple(key []byte) []Node
	IterTuples(f func(key []byte, host []*Host) bool)
}

// SerfTransport implements a Transport interface using serf for gossip
type SerfTransport struct {
	serfConf *serf.Config
	serf     *serf.Serf

	// Timeout for serf requests
	queryTimeout time.Duration

	// Used to service rpc requests
	local AffinityGroupRPC

	// Gossip events from serf
	events chan serf.Event

	// write to this channel to initiate shutdown
	shutdown chan struct{}
}

// NewSerfTransport inits a new serf backed gossip transport with a default
// query timeout of 3 seconds
func NewSerfTransport(serfConf *serf.Config) (*SerfTransport, error) {
	trans := &SerfTransport{
		events:       make(chan serf.Event, 64),
		serfConf:     serfConf,
		queryTimeout: 3 * time.Second,
	}
	trans.serfConf.EventCh = trans.events

	s, err := serf.Create(serfConf)
	if err != nil {
		return nil, err
	}
	trans.serf = s

	return trans, nil
}

// Serf returns the underlying serf instance
func (trans *SerfTransport) Serf() *serf.Serf {
	return trans.serf
}

// Ping gets the coordinates of the remote and local node and retursn the
// distance ie responcse time.  If the supplied node is the node itself or an
// error occurs 0 is returned
func (trans *SerfTransport) Ping(node *Node) time.Duration {
	// 0 for a self ping
	if node.Name == trans.serf.LocalMember().Name {
		return 0
	}

	remote, ok := trans.serf.GetCachedCoordinate(node.String())
	if !ok {
		// Directly ping remote and get rtt if not found in cache
		addr := &net.TCPAddr{IP: node.Addr, Port: int(node.Port)}
		mlist := trans.serf.Memberlist()
		rtt, _ := mlist.Ping(node.Name, addr)
		return rtt
	}

	// Get self coords to calculate rtt
	self, err := trans.serf.GetCoordinate()
	if err == nil {
		return self.DistanceTo(remote)
	}

	//log.Println("[ERROR] Failed to get self coordinates:", err)
	return 0
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

// Join tries to the the given peers
func (trans *SerfTransport) Join(peers ...string) error {
	_, err := trans.serf.Join(peers, true)
	return err
}

// Register registers the local rpc used to serve network requests
func (trans *SerfTransport) Register(rpc AffinityGroupRPC) {
	trans.local = rpc
	go trans.handleEvents()
}

func (trans *SerfTransport) handleEvents() {
	var err error

	for {
		select {

		case event := <-trans.events:
			typ := event.EventType()
			switch typ {

			case serf.EventQuery:
				evt := event.(*serf.Query)
				err = trans.handleQuery(evt)

			case serf.EventUser:
				evt := event.(serf.UserEvent)
				err = trans.handleUser(evt)

			case serf.EventMemberJoin:
				evt := event.(serf.MemberEvent)
				err = trans.handleJoin(evt)

			case serf.EventMemberFailed, serf.EventMemberLeave:
				evt := event.(serf.MemberEvent)
				err = trans.handleMemLeftOrFailed(evt)

			case serf.EventMemberReap, serf.EventMemberUpdate:
				evt := event.(serf.MemberEvent)
				log.Println("[DEBUG]", typ, event, evt.Members)

			}

			if err != nil {
				log.Printf("[ERROR] event=%s error=%v", typ, err)
			}

		case <-trans.shutdown:
			return
		}
	}

}

// handleJoin adds the nodes to their associated affinity groups.  It
// returns the last encountered error
func (trans *SerfTransport) handleJoin(event serf.MemberEvent) (err error) {
	for _, mem := range event.Members {

		node := &Node{
			Name:       mem.Name,
			Tags:       mem.Tags,
			Host:       &Host{mem.Addr, mem.Port},
			LastSeen:   time.Now(),
			Heartbeats: 1,
		}

		if er := trans.local.AddNode(node); er != nil {
			err = er
		}

	}
	return
}

// handleMemLeftOrFailed removes the failed node from its associated AffinityGroup.
// It returns the last encountered error
func (trans *SerfTransport) handleMemLeftOrFailed(event serf.MemberEvent) (err error) {
	for _, mem := range event.Members {

		host := fmt.Sprintf("%s:%d", mem.Addr, mem.Port)
		if er := trans.local.RemoveNode(host); er != nil {
			err = er
		}

	}

	return
}

func (trans *SerfTransport) handleUser(evt serf.UserEvent) error {
	data := evt.Payload
	typ := evt.Name
	switch typ {

	case userInsert:
		pos := len(data) - 18

		key := data[:pos]
		host := NewHostFromBytes(data[pos:])
		// Add conditionally
		trans.local.AddTuple(key, host)
	}

	return nil
}

func (trans *SerfTransport) handleQuery(query *serf.Query) (err error) {
	typ := query.Name

	switch typ {

	case queryLookup:
		nodes := trans.local.GetTuple(query.Payload)
		var buf []byte
		if buf, err = msgpack.Marshal(nodes); err == nil {
			err = query.Respond(buf)
		}

	}

	return err
}

// Shutdown signals a shutdown of the even channel
func (trans *SerfTransport) Shutdown() {
	trans.shutdown <- struct{}{}
}
