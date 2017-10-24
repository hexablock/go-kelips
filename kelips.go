package kelips

import (
	"log"
	"time"

	"github.com/hashicorp/serf/serf"
)

// TupleStore implements a store for a mapping of files to hosts
type TupleStore interface {
	Add(name string, host *Host) bool
	Get(name string) []*Host
	Del(name string, host *Host) bool
}

// Transport needed by kelips
type Transport interface {
	Ping(node *Node) time.Duration
	// Insert a key-host association
	Insert(key []byte, host *Host) error
	// Lookup a key on the optional hosts
	Lookup(key []byte, hosts ...string) ([]Node, error)
}

// Kelips is the core kelips struct that maintains the internal state
type Kelips struct {
	// Local node information
	node Node

	// This nodes group index
	idx int

	// Kelips config
	conf *Config

	// Gossip config
	serfConf *serf.Config

	// Gossip transport
	serf *serf.Serf

	// All affinity groups in the dht
	groups affinityGroups

	// file or key tuples
	tuples TupleStore

	// Events received via gossip used to to update state
	events chan serf.Event

	// Channel to block for completion
	stopped chan struct{}

	trans Transport
}

// NewKelips initializes serf and starts the event handlers
func NewKelips(conf *Config, serfConf *serf.Config) (*Kelips, error) {
	mc := serfConf.MemberlistConfig
	node := Node{Host: NewHost(mc.AdvertiseAddr, uint16(mc.AdvertisePort))}
	k := &Kelips{
		node:     node,
		conf:     conf,
		serfConf: serfConf,
		events:   make(chan serf.Event, 64),
		stopped:  make(chan struct{}, 1),
		tuples:   NewInmemTuples(),
	}
	k.init()

	s, err := serf.Create(serfConf)
	if err != nil {
		return nil, err
	}

	k.serf = s
	k.trans = NewSerfTransport(s)
	for _, v := range k.groups {
		v.trans = k.trans
	}

	go k.handleEvents()
	go k.checkNodes()

	return k, nil
}

func (kelps *Kelips) init() {
	h := kelps.conf.HashFunc()
	k := int64(kelps.conf.NumAffinityGroups)
	// Init local node
	kelps.node.init(kelps.conf.HashFunc)
	// Set channel to receive events on
	kelps.serfConf.EventCh = kelps.events
	// Init affinity groups
	kelps.groups = genAffinityGroups(k, int64(h.Size()))
	// Set local group index
	group := kelps.groups.get(kelps.node.ID)
	kelps.idx = group.index

	log.Printf("[INFO] Kelips initialized group=%d id=%x total=%d",
		group.index, kelps.node.ID, len(kelps.groups))
}

func (kelps *Kelips) checkNodes() {
	for {
		time.Sleep(kelps.conf.HeartbeatInterval)
		for _, grp := range kelps.groups {
			grp.checkNodes()
		}
	}
}

// LookupGroup hashes a key and returning the associated group
func (kelps *Kelips) LookupGroup(key []byte) AffinityGroup {
	h := kelps.conf.HashFunc()
	h.Write(key)
	sh := h.Sum(nil)

	return kelps.groups.get(sh[:])
}

// Lookup does a local lookup and returns all nodes that have the key.  If the
// key is not found no nodes are returned
func (kelps *Kelips) Lookup(key []byte) ([]Node, error) {
	grp := kelps.LookupGroup(key)

	var (
		nodes []Node
		err   error
	)

	if grp.Index() == kelps.idx {
		// We own the key - return nodes we have for it
		nodes = kelps.getTupleNodes(grp, key)

	} else {
		n := grp.Nodes()
		// Filter by nodes in the group
		filter := make([]string, 0, len(n))
		for _, v := range n {
			filter = append(filter, v.Name)
		}

		nodes, err = kelps.trans.Lookup(key, filter...)
	}

	return nodes, err
}

// Insert hashes the key and inserts the key host pair into the associated
// affinity group.  If the local node is part of the group the tuple is stored.
// The insert is then broadcasted to the network.
func (kelps *Kelips) Insert(key []byte, host *Host) error {
	grp := kelps.LookupGroup(key)

	if grp.Index() == kelps.idx {
		// Add to our local store as we are in the group
		kelps.tuples.Add(string(key), host)
	}
	// Broadcast the insert to the network.
	return kelps.trans.Insert(key, host)
}

// Join issues a join to the gossip network
func (kelps *Kelips) Join(existing ...string) error {
	_, err := kelps.serf.Join(existing, false)
	return err
}

// Serf returns the underlying serf instance i.e. the gossip transport
func (kelps *Kelips) Serf() *serf.Serf {
	return kelps.serf
}

// Leave instructs the node to leave the gossip network.  This ensures a clean
// shutdown
func (kelps *Kelips) Leave() error {
	return kelps.serf.Leave()
}

// Shutdown shutdowns down the dht.  This is a force shutdown as such Leave
// should be called before this for a clean shutdown.
func (kelps *Kelips) Shutdown() error {
	err := kelps.serf.Shutdown()
	return err
}

func (kelps *Kelips) getTupleNodes(grp AffinityGroup, key []byte) []Node {
	hosts := kelps.tuples.Get(string(key))

	nodes := make([]Node, 0, len(hosts))
	for _, h := range hosts {
		if n, ok := grp.GetNode(h.String()); ok {
			nodes = append(nodes, n)
		}
	}

	return nodes
}
