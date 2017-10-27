package kelips

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
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

	Register(AffinityGroupRPC)
}

// Kelips is the core kelips struct that maintains the internal state
type Kelips struct {
	// Local node information
	node Node

	// Kelips config
	conf *Config

	// affinity groups
	group *localAffinityGroup

	// Gossip transport
	trans Transport
}

// NewKelips initializes serf and starts the event handlers
func NewKelips(conf *Config, trans Transport) (*Kelips, error) {
	addr, port, err := parseAdvAddr(conf.Hostname)
	if err != nil {
		return nil, err
	}

	tuples := NewInmemTuples()

	k := &Kelips{
		node:  Node{Host: NewHost(addr, uint16(port))},
		conf:  conf,
		group: newLocalAffinityGroup(tuples, conf.HashFunc),
		trans: trans,
	}
	k.init()

	// Start node checker
	go k.checkNodes()

	return k, nil
}

func (kelps *Kelips) LocalHost() *Host {
	return kelps.node.Host
}

func (kelps *Kelips) init() {
	k := int64(kelps.conf.NumAffinityGroups)

	// Init local id
	kelps.node.init(kelps.conf.HashFunc)
	kelps.group.init(kelps.node.ID, k, kelps.trans)

	kelps.trans.Register(kelps.group)

	log.Printf("[INFO] Kelips initialized group=%d id=%x total=%d",
		kelps.group.idx, kelps.node.ID, len(kelps.group.groups))
}

func (kelps *Kelips) checkNodes() {
	for {
		time.Sleep(kelps.conf.HeartbeatInterval)
		kelps.group.checkNodes()
	}
}

// LookupGroup hashes a key and returning the associated group
func (kelps *Kelips) LookupGroup(key []byte) AffinityGroup {
	return kelps.group.getGroup(key)
}

// Lookup does a local lookup and returns all nodes that have the key.  If the
// key is not found no nodes are returned
func (kelps *Kelips) Lookup(key []byte) ([]Node, error) {
	grp := kelps.group.getGroup(key)

	var (
		nodes []Node
		err   error
	)

	if grp.Index() == kelps.group.idx {
		// We own the key - return nodes we have for it
		nodes = kelps.group.getTupleNodes(grp, key)

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
	// Conditionally add tuple
	kelps.group.AddTuple(string(key), host)
	// Broadcast the insert to the network.
	return kelps.trans.Insert(key, host)
}

func parseAdvAddr(addr string) (string, int64, error) {
	hp := strings.Split(addr, ":")
	if len(hp) < 2 {
		return "", 0, fmt.Errorf("invalid advertise address")
	}
	port, err := strconv.ParseInt(hp[len(hp)-1], 10, 32)
	return hp[0], port, err
}
