package kelips

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"time"

	"github.com/hexablock/hexatype"
	"github.com/hexablock/log"
	"github.com/hexablock/vivaldi"
)

// Config holds the kelips config to initialize the dht
type Config struct {
	// AdvertiseHost used to compute the hash.  This may be different from the
	// transport address
	AdvertiseHost string

	// Number of affinity groups i.e. k value
	NumGroups int

	// Setting this to true will cause writes to be propogated to all nodes in
	// in that group
	EnablePropogation bool

	// Hash function to use
	HashFunc func() hash.Hash

	Region string
	Sector string
	Zone   string

	// Meta is serialized to binary and made part of the node object
	Meta map[string]string
}

// DefaultConfig returns a minimum required config
func DefaultConfig(host string) *Config {
	return &Config{
		AdvertiseHost: host,
		NumGroups:     2,
		HashFunc:      sha256.New,
		Region:        "region1",
		Sector:        "sector1",
		Zone:          "zone1",
		Meta:          make(map[string]string),
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
	LookupNodes(key []byte, min int) ([]*hexatype.Node, error)

	LookupGroupNodes(key []byte) ([]*hexatype.Node, error)

	// Lookup nodes from the local view
	Lookup(key []byte) ([]*hexatype.Node, error)

	// Insert to local group
	Insert(key []byte, tuple TupleHost, propogate bool) error

	// Delete from local group
	Delete(key []byte, tuple TupleHost, propogate bool) error
}

// Transport implements RPC's needed by kelips
type Transport interface {
	LookupGroupNodes(host string, key []byte) ([]*hexatype.Node, error)
	Lookup(host string, key []byte) ([]*hexatype.Node, error)
	Insert(host string, key []byte, tuple TupleHost, propogate bool) error
	Delete(host string, key []byte, tuple TupleHost, propogate bool) error
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

	//if conf.EnablePropogation {
	log.Println("[INFO] Kelips write propogation enabled!")
	go k.local.propogate(conf.HashFunc)
	//}

	return k
}

func (kelips *Kelips) init() {
	c := kelips.conf
	kelips.groups = genAffinityGroups(int64(c.NumGroups), int64(c.HashFunc().Size()))

	tuple := NewTupleHost(kelips.conf.AdvertiseHost)
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
		local:    localNode,
		idx:      local.index,
		tuples:   kelips.tuples,
		groups:   kelips.groups,
		hashFunc: c.HashFunc,
		trans:    kelips.trans,
		propReqs: make(chan *propReq, 32),
	}

	kelips.trans.Register(kelips.local)

	log.Printf("[INFO] Kelips node=%x", localNode.ID)
	log.Printf("[INFO] Kelips group=%d/%d id=%x", local.index, c.NumGroups, local.id)
}

// LocalNode returns the local node by performing a lookup
func (kelips *Kelips) LocalNode() hexatype.Node {
	group := kelips.groups[kelips.local.idx]
	n, _ := group.getNode(kelips.conf.AdvertiseHost)
	return *n
}

// Insert inserts a key and associated tuple.  If the key belongs to a foreign
// group, the insert is forwarded to a node in that group and its response
// is returned.  If the TupleHost is not known it will not be returned in a
// lookup though will still be in the tuple store.  Once the node is known/alive
// it will be returned in lookups
func (kelips *Kelips) Insert(key []byte, tuple TupleHost) error {
	h := kelips.conf.HashFunc()

	// Hash key
	h.Write(key)
	keysh := h.Sum(nil)

	// Get key group
	group := kelips.groups.get(keysh)
	if group.index == kelips.local.idx {
		return kelips.local.Insert(key, tuple, true)
	}

	// Handle foreign group
	group = kelips.groups.nextClosestGroup(group)
	if group == nil {
		return fmt.Errorf("no nodes found for key: %x", key)
	}
	nodes := group.Nodes()

	var err error
	for _, n := range nodes {
		// Return on first successful one
		if er := kelips.trans.Insert(n.Host(), key, tuple, true); er != nil {
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
func (kelips *Kelips) Delete(key []byte, tuple TupleHost) (err error) {
	h := kelips.conf.HashFunc()

	// Hash key
	h.Write(key)
	keysh := h.Sum(nil)

	// Get key group
	group := kelips.groups.get(keysh)
	if group.index == kelips.local.idx {
		err = kelips.local.Delete(key, tuple, true)
		return err
	}

	// Handle foreign group
	nodes := group.Nodes()
	for _, n := range nodes {
		// First successful one
		if er := kelips.trans.Delete(n.Host(), key, tuple, true); er != nil {
			err = er
			continue
		}
		return nil
	}

	return err
}

// LookupNodes returns a minimum of n nodes that a key maps to
func (kelips *Kelips) LookupNodes(key []byte, min int) ([]*hexatype.Node, error) {
	return kelips.local.LookupNodes(key, min)
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

	// Handle foreign group
	// group = kelips.groups.nextClosestGroup(group)
	// if group == nil {
	// 	return nil, fmt.Errorf("nodes not found: %x", key)
	// }
	nodes := group.Nodes()

	var err error
	for _, n := range nodes {
		// First successful one
		hosts, er := kelips.trans.Lookup(n.Host(), key)
		if er == nil {
			return hosts, nil
			// lnodes := make([]hexatype.Node, len(hosts))
			// for i := range hosts {
			// 	lnodes[i] = *hosts[i]
			// }
			// return lnodes, nil
		}
		err = er
	}

	return nil, err
}

// PingNode sets the coords and rtt on a node and updates the heartbeat count
func (kelips *Kelips) PingNode(hostname string, coord *vivaldi.Coordinate, rtt time.Duration) error {
	group := kelips.getHostGroup(hostname)
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
	group := kelips.getHostGroup(hostname)
	if group.index == kelips.local.idx {
		// If local remove all tuple references
		kelips.tuples.ExpireHost(NewTupleHost(hostname))
	}
	// Remove node from group
	return group.removeNode(hostname)
}

func (kelips *Kelips) getHostGroup(host string) *affinityGroup {
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
			tupleHost := TupleHost(host)
			if er := kelips.Insert(tuple.Key, tupleHost); er != nil {
				err = er
			}
		}
	}

	return err
}
