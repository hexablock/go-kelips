package kelips

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"time"

	"github.com/hexablock/hexatype"
	"github.com/hexablock/vivaldi"
)

// Config holds the kelips config to initialize the dht
type Config struct {
	// Hostname used to compute the hash.  This may be different from the
	// transport address
	Hostname string

	// Number of affinity groups i.e. k value
	NumGroups int

	// Hash function to use
	HashFunc func() hash.Hash

	Region string
	Sector string
	Zone   string
	Meta   map[string]string
}

// DefaultConfig returns a minimum required config
func DefaultConfig(host string) *Config {
	return &Config{
		Hostname:  host,
		NumGroups: 2,
		HashFunc:  sha256.New,
		Region:    "region1",
		Sector:    "sector1",
		Zone:      "zone1",
		Meta:      make(map[string]string),
	}
}

func (conf *Config) metaBytes() []byte {
	if len(conf.Meta) == 0 {
		return nil
	}
	var out string
	for k, v := range conf.Meta {
		out += fmt.Sprintf("%s=%s,", k, v)
	}
	return []byte(out[:len(out)-1])
}

// AffinityGroupRPC implements an interface for local rpc's used by the
// transport
type AffinityGroupRPC interface {
	LookupGroupNodes(key []byte) ([]*hexatype.Node, error)

	// Lookup nodes from the local view
	Lookup(key []byte) ([]*hexatype.Node, error)

	// Insert to local group
	Insert(key []byte, tuple TupleHost) error

	// Delete from local group
	Delete(key []byte) error
}

// Transport implements RPC's needed by kelips
type Transport interface {
	LookupGroupNodes(host string, key []byte) ([]*hexatype.Node, error)
	Lookup(host string, key []byte) ([]*hexatype.Node, error)
	Insert(host string, key []byte, tuple TupleHost) error
	Delete(host string, key []byte) error
	Register(AffinityGroupRPC)
}

// AffinityGroup is an interface used for group lookups.  This is used to get
// nodes in a group
type AffinityGroup interface {
	Nodes() []hexatype.Node
}

// Kelips is the core engine of the dht.  It provides functions to operate
// against it.  The gossip layer is not included as part of the implementation
// but rather methods have been provided such that they can be called from
// a gossip interface
type Kelips struct {
	conf *Config

	// All groups
	groups affinityGroups

	// local affinity group
	local *localGroup

	// Key tuples
	tuples *InmemTuples

	// Network transport
	trans Transport
}

// Create instantiates kelips and registers the local group to the transport
func Create(conf *Config, remote Transport) *Kelips {
	k := &Kelips{
		conf:   conf,
		tuples: NewInmemTuples(),
		trans:  remote,
	}

	k.init()

	return k
}

func (kelips *Kelips) init() {
	c := kelips.conf
	kelips.groups = genAffinityGroups(int64(c.NumGroups), int64(c.HashFunc().Size()))

	tuple := NewTupleHost(kelips.conf.Hostname)
	localNode := &hexatype.Node{
		Address: tuple,
		Region:  kelips.conf.Region,
		Sector:  kelips.conf.Sector,
		Zone:    kelips.conf.Zone,
		Meta:    kelips.conf.metaBytes(),
	}
	localNode.ID = localNode.HashID(c.HashFunc())

	local := kelips.groups.get(localNode.ID)
	local.addNode(localNode, true)

	kelips.local = &localGroup{
		idx:      local.index,
		tuples:   kelips.tuples,
		groups:   kelips.groups,
		hashFunc: c.HashFunc,
	}

	kelips.trans.Register(kelips.local)

}

// Insert inserts a key and associated tuple.  If the key belongs to a foreign
// group, the insert is forwarded to a node in that group and its response
// is returned.  If the TupleHost is not known it will not be returned in a
// lookup though will still be in the tuple store.  Once the node is known/alive
// it will be returned in lookups
func (kelips *Kelips) Insert(key []byte, tuple TupleHost) (err error) {
	h := kelips.conf.HashFunc()

	// Hash key
	h.Write(key)
	keysh := h.Sum(nil)

	// Get key group
	group := kelips.groups.get(keysh)
	if group.index == kelips.local.idx {
		return kelips.local.Insert(key, tuple)
	}

	// Handle foreign group
	nodes := group.Nodes()
	for _, n := range nodes {
		// First successful one
		if er := kelips.trans.Insert(n.Host(), key, tuple); er != nil {
			err = er
			continue
		}
		return nil
	}

	return err
}

// Delete deletes a key and all assoicated tuples.  If the key belongs to a
// foreign group the delete is forwarded to a node in that group and its
// response is returned
func (kelips *Kelips) Delete(key []byte) (err error) {
	h := kelips.conf.HashFunc()

	// Hash key
	h.Write(key)
	keysh := h.Sum(nil)

	// Get key group
	group := kelips.groups.get(keysh)
	if group.index == kelips.local.idx {
		return kelips.local.Delete(key)
	}

	// Handle foreign group
	nodes := group.Nodes()
	for _, n := range nodes {
		// First successful one
		if er := kelips.trans.Delete(n.Host(), key); er != nil {
			err = er
			continue
		}
		return nil
	}

	return err
}

// LocalNode returns the local node by performing a lookup
func (kelips *Kelips) LocalNode() hexatype.Node {
	group := kelips.groups[kelips.local.idx]
	n, _ := group.getNode(kelips.conf.Hostname)
	return *n
}

// LookupGroupNodes returns all nodes in a group for the key
func (kelips *Kelips) LookupGroupNodes(key []byte) ([]*hexatype.Node, error) {
	return kelips.local.LookupGroupNodes(key)
}

// Lookup hashes the key and finds its affinity group.  If the group is local
// it returns all local known nodes for the key otherwise it makes a Lookup call
// on the owning foreign group and returns its nodes
func (kelips *Kelips) Lookup(key []byte) ([]*hexatype.Node, error) {
	h := kelips.conf.HashFunc()
	h.Write(key)
	sh := h.Sum(nil)

	group := kelips.groups.get(sh)
	if group.index == kelips.local.idx {
		return kelips.local.Lookup(key)
	}

	nodes := group.Nodes()
	var err error
	for _, n := range nodes {
		// First successful one
		hosts, er := kelips.trans.Lookup(n.Host(), key)
		if er == nil {
			return hosts, nil
		}
		err = er
	}

	return nil, err
}

// PingNode sets the coords and rtt on a node and updates the heartbeat count
func (kelips *Kelips) PingNode(hostname string, coord *vivaldi.Coordinate, rtt time.Duration) error {
	group := kelips.getGroup(hostname)
	return group.pingNode(hostname, coord, rtt)
}

// AddNode adds a node to the DHT.  It calculates the node id and adds it to the
// group it belongs to
func (kelips *Kelips) AddNode(node *hexatype.Node, force bool) error {
	node.ID = node.HashID(kelips.conf.HashFunc())
	group := kelips.groups.get(node.ID)
	return group.addNode(node, force)
}

// RemoveNode removes a node from the DHT.  This will remove the node from the
// local nodes view as well as remove all references in the tuples
func (kelips *Kelips) RemoveNode(hostname string) error {
	group := kelips.getGroup(hostname)
	if group.index == kelips.local.idx {
		// If local remove all tuple references
		kelips.tuples.ExpireHost(NewTupleHost(hostname))
	}
	// Remove node from group
	return group.removeNode(hostname)
}

func (kelips *Kelips) getGroup(host string) *affinityGroup {
	tuple := NewTupleHost(host)
	id := tuple.ID(kelips.conf.HashFunc())
	return kelips.groups.get(id)
}

// Snapshot returns a Snapshot of all tuples and nodes known to the node.
func (kelips *Kelips) Snapshot() *Snapshot {
	ss := kelips.local.Snapshot()
	ss.Groups = int32(kelips.conf.NumGroups)

	return ss
}

// Seed seeds the local groups with the given snapshot
func (kelips *Kelips) Seed(snapshot *Snapshot) error {
	var err error

	for _, node := range snapshot.Nodes {
		if er := kelips.AddNode(node, true); er != nil {
			if er == errNodeExists {
				continue
			}
			err = er
		}
	}

	for _, tuple := range snapshot.Tuples {
		for _, host := range tuple.Hosts {
			kelips.tuples.Insert(tuple.Key, TupleHost(host))
		}
	}

	return err
}
