package kelips

import (
	"fmt"
	"log"
	"time"

	"github.com/hexablock/go-kelips/kelipspb"
	"github.com/hexablock/iputil"
	"github.com/hexablock/vivaldi"
)

// TupleStore is used to store and interact with tuples
type TupleStore interface {
	// Iter iterates over all the tuples.  If the callback returns false, iteration
	// is terminated
	Iter(f func(key []byte, hosts []TupleHost) bool)

	// Count returns the total number of keys in the store
	Count() int

	// Insert adds a new host for a name if it does not already exist.  It returns true
	// if the host was added
	Insert(key []byte, h TupleHost) error

	// Delete deletes a key removing all associated TupleHosts
	Delete(key []byte) error

	// Get returns a list of hosts for a key.  It returns nil if the name is not
	// found
	Get(key []byte) ([]TupleHost, error)

	// DeleteKeyHost deletes a host associated to the name returning true if it was deleted
	DeleteKeyHost(key []byte, h TupleHost) bool

	// ExpireHost removes a host from all keys referring to it
	ExpireHost(tuple TupleHost) bool
}

// AffinityGroupRPC implements an interface for local rpc's used by the
// transport
type AffinityGroupRPC interface {
	// Lookup nodes returning the min amount
	LookupNodes(key []byte, min int) ([]*kelipspb.Node, error)

	// Return all nodes for key group
	LookupGroupNodes(key []byte) ([]*kelipspb.Node, error)

	// Lookup nodes from the local view
	Lookup(key []byte) ([]*kelipspb.Node, error)

	// Insert to local group
	Insert(key []byte, tuple TupleHost, propogate bool) error

	// Delete from local group
	Delete(key []byte, tuple TupleHost, propogate bool) error
}

// Transport implements RPC's needed by kelips
type Transport interface {
	LookupGroupNodes(host string, key []byte) ([]*kelipspb.Node, error)
	Lookup(host string, key []byte) ([]*kelipspb.Node, error)
	Insert(host string, key []byte, tuple TupleHost, propogate bool) error
	Delete(host string, key []byte, tuple TupleHost, propogate bool) error
	// Register a local affinity group
	Register(AffinityGroupRPC)
}

// AffinityGroup is an interface used for group lookups.  This is used to get
// nodes in a group
type AffinityGroup interface {
	Nodes() []kelipspb.Node
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
	tuples TupleStore

	// Network transport
	trans Transport
}

// Create instantiates kelips and registers the local group to the transport. It
// inits an in-memory tuple store
func Create(conf *Config, remote Transport) *Kelips {
	k := &Kelips{
		conf:   conf,
		tuples: conf.TupleStore,
		trans:  remote,
	}

	if k.tuples == nil {
		k.tuples = NewInmemTuples()
	}

	k.init()

	if conf.EnablePropogation {
		log.Println("[INFO] Kelips write propogation enabled!")
		go k.local.propogate(conf.HashFunc)
	}

	return k
}

func (kelips *Kelips) init() {
	c := kelips.conf
	// Build affinity groups
	kelips.groups = genAffinityGroups(int64(c.K), int64(c.HashFunc().Size()))

	localNode := &kelipspb.Node{
		Address: kelipspb.NewAddress(c.AdvertiseHost), //kelipspb.Address(tuple),
		Meta: map[string]string{
			"region": c.Region,
			"sector": c.Sector,
			"zone":   c.Zone,
		},
	}
	localNode.ID = localNode.HashID(c.HashFunc())

	group := kelips.groups.get(localNode.ID)
	group.addNode(localNode, true)

	// Build local group
	kelips.local = &localGroup{
		local:    localNode,
		idx:      group.index,
		tuples:   kelips.tuples,
		groups:   kelips.groups,
		hashFunc: c.HashFunc,
		trans:    kelips.trans,
		propReqs: make(chan *propReq, 32),
	}

	kelips.trans.Register(kelips.local)

	log.Printf("[INFO] Kelips node=%x", localNode.ID)
	log.Printf("[INFO] Kelips group=%d/%d id=%x", group.index, c.K, group.id)
}

// Join adds the given peers to their respective groups
func (kelips *Kelips) Join(peers []string) error {
	var err error
	for i := range peers {
		addr, port, er := iputil.SplitHostPort(peers[i])
		if er != nil {
			err = er
			continue
		}
		node := kelipspb.NewNode(addr, port)
		if er = kelips.AddNode(node, false); er != nil {
			err = er
		}
	}
	return err
}

// LocalNode returns the local node by performing a lookup
func (kelips *Kelips) LocalNode() kelipspb.Node {
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

	// Local group
	if group.index == kelips.local.idx {
		return kelips.local.Insert(key, tuple, true)
	}

	// Foreign group
	group = kelips.groups.nextClosestGroup(group)
	if group == nil {
		return fmt.Errorf("no nodes found for key: %x", key)
	}
	nodes := group.Nodes()

	var err error
	for _, n := range nodes {
		// Return on first successful one
		if er := kelips.trans.Insert(n.Address.String(), key, tuple, true); er != nil {
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
		if er := kelips.trans.Delete(n.Address.String(), key, tuple, true); er != nil {
			err = er
			continue
		}
		return nil
	}

	return err
}

// LookupNodes returns a minimum of n nodes that a key maps to
func (kelips *Kelips) LookupNodes(key []byte, min int) ([]*kelipspb.Node, error) {
	return kelips.local.LookupNodes(key, min)
}

// LookupGroupNodes returns all nodes in a group for the key
func (kelips *Kelips) LookupGroupNodes(key []byte) ([]*kelipspb.Node, error) {
	return kelips.local.LookupGroupNodes(key)
}

// Lookup hashes the key and finds its affinity group.  If the group is local
// it returns all local known nodes for the key otherwise it makes a Lookup call
// on the owning foreign group and returns its nodes
func (kelips *Kelips) Lookup(key []byte) ([]*kelipspb.Node, error) {
	h := kelips.conf.HashFunc()
	h.Write(key)
	sh := h.Sum(nil)

	group := kelips.groups.get(sh)
	nodes := group.Nodes()
	if len(nodes) == 0 {
		group = kelips.groups.nextClosestGroup(group)
		if group == nil {
			return nil, fmt.Errorf("no nodes found for key: %x", key)
		}
	}

	if group.index == kelips.local.idx {
		return kelips.local.Lookup(key)
	}

	// Get the first successful list from any node in the other group
	var err error

	nodes = group.Nodes()
	for _, n := range nodes {

		hosts, er := kelips.trans.Lookup(n.Address.String(), key)
		if er == nil {
			// Found
			return hosts, nil
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
func (kelips *Kelips) AddNode(node *kelipspb.Node, force bool) error {
	node.ID = node.HashID(kelips.conf.HashFunc())
	group := kelips.groups.get(node.ID)
	return group.addNode(node, force)
}

// RemoveNode removes a node from the DHT.  This will remove the node from the
// local nodes view as well as remove all references in the tuples
func (kelips *Kelips) RemoveNode(hostname string) error {
	group := kelips.getHostGroup(hostname)
	if group.index == kelips.local.idx {
		// If local remove all tuple references before actually removing the
		// node.
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
func (kelips *Kelips) Snapshot() *kelipspb.Snapshot {
	ss := kelips.local.Snapshot()
	ss.Groups = int32(kelips.conf.K)

	return ss
}

// Seed seeds the local groups with the given snapshot
func (kelips *Kelips) Seed(snapshot *kelipspb.Snapshot) error {
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
