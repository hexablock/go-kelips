package kelips

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"log"
	"sync"
	"time"
)

var (
	errNodeNotFound = errors.New("node not found")
)

type localAffinityGroup struct {
	idx int

	groups affinityGroups
	tuples TupleStore

	hashFunc func() hash.Hash
}

func newLocalAffinityGroup(tuples TupleStore, hf func() hash.Hash) *localAffinityGroup {
	return &localAffinityGroup{
		tuples:   tuples,
		hashFunc: hf}
}

func (ag *localAffinityGroup) init(nid []byte, k int64, trans Transport) {
	ag.groups = genAffinityGroups(k, int64(ag.hashFunc().Size()))
	for _, g := range ag.groups {
		g.trans = trans
	}
	// Set local group index
	group := ag.groups.get(nid)
	ag.idx = group.index
}

// check all the nodes
func (ag *localAffinityGroup) checkNodes() {
	for _, grp := range ag.groups {
		grp.checkNodes()
	}
}

func (ag *localAffinityGroup) getGroup(key []byte) *affinityGroup {
	h := ag.hashFunc()
	h.Write(key)
	sh := h.Sum(nil)
	return ag.groups.get(sh[:])
}

func (ag *localAffinityGroup) AddNode(node *Node) error {
	node.init(ag.hashFunc())
	group := ag.groups.get(node.ID)
	return group.addNode(node)
}

func (ag *localAffinityGroup) RemoveNode(host string) error {
	group := ag.getGroup([]byte(host))
	return group.removeNode(host)
}

// Add tuple adds a tuple to the associated group only if it is this nodes group
func (ag *localAffinityGroup) AddTuple(name string, host *Host) {
	group := ag.getGroup([]byte(name))
	if group.Index() != ag.idx {
		return
	}

	ag.tuples.Add(name, host)

}

func (ag *localAffinityGroup) Lookup(key []byte) []Node {
	group := ag.getGroup(key)
	// Only process if we are in the group
	if group.Index() != ag.idx {
		return nil
	}
	return ag.getTupleNodes(group, key)
}

func (ag *localAffinityGroup) GetTuples(key []byte) []Node {
	grp := ag.getGroup(key)
	return ag.getTupleNodes(grp, key)
}

func (ag *localAffinityGroup) getTupleNodes(grp AffinityGroup, key []byte) []Node {
	hosts := ag.tuples.Get(string(key))
	nodes := make([]Node, 0, len(hosts))
	for _, h := range hosts {

		// Check local group
		n, ok := grp.GetNode(h.String())
		if !ok {
			// Check other groups
			rgrp := ag.getGroup([]byte(h.String()))
			if n, ok = rgrp.GetNode(h.String()); !ok {
				// Continue/skip if not found
				continue
			}
		}
		nodes = append(nodes, n)

	}

	return nodes
}

// AffinityGroup implements an affinity group interface to abstract local and
// remote groups
type AffinityGroup interface {
	ID() []byte
	Index() int
	Nodes() []Node
	GetNode(hostname string) (Node, bool)
}

// affinityGroup is a partial view of the nodes part of a given affinity group
type affinityGroup struct {
	// id constructed by dividing the hash keyspace by NumAffinityGroups
	id []byte
	// k value of this group
	index int
	// nodes part of the affinity group
	mu sync.RWMutex
	m  map[string]*Node

	trans Transport
}

func newAffinityGroup(id []byte, index int) *affinityGroup {
	return &affinityGroup{
		id:    id,
		index: index,
		m:     make(map[string]*Node),
	}
}

// ID returns the group hash id
func (group *affinityGroup) ID() []byte {
	return group.id
}

// Index returns the group index
func (group *affinityGroup) Index() int {
	return group.index
}

func (group *affinityGroup) GetNode(hostname string) (Node, bool) {
	group.mu.RLock()
	defer group.mu.RUnlock()

	n, ok := group.m[hostname]
	if ok {
		return *n, ok
	}
	return Node{}, false
}

func (group *affinityGroup) Nodes() []Node {
	group.mu.RLock()
	n := make([]Node, 0, len(group.m))
	for _, node := range group.m {
		n = append(n, *node)
	}
	group.mu.RUnlock()
	return n
}

// pingNode updates the heartbeat count, rtt, and last seen values
func (group *affinityGroup) pingNode(hostname string, rtt time.Duration) error {
	group.mu.RLock()
	node, ok := group.m[hostname]
	if !ok {
		group.mu.RUnlock()
		return errNodeNotFound
	}
	group.mu.RUnlock()

	group.mu.Lock()
	node.Heartbeats++
	node.LastSeen = time.Now()
	node.RTT = rtt
	group.m[hostname] = node
	group.mu.Unlock()

	return nil
}

func (group *affinityGroup) removeNode(hostname string) error {
	group.mu.RLock()
	_, ok := group.m[hostname]
	if !ok {
		group.mu.RUnlock()
		return errNodeNotFound
	}
	group.mu.RUnlock()

	group.mu.Lock()
	delete(group.m, hostname)
	group.mu.Unlock()

	log.Printf("[INFO] Node removed group=%d count=%d node=%s", group.index,
		len(group.m), hostname)

	return nil
}

func (group *affinityGroup) addNode(node *Node) error {

	group.mu.RLock()
	if _, ok := group.m[node.String()]; ok {
		group.mu.RUnlock()
		return fmt.Errorf("node exists")
	}
	group.mu.RUnlock()

	group.mu.Lock()
	node.Heartbeats = 1
	group.m[node.String()] = node
	group.mu.Unlock()

	log.Printf("[INFO] Node added group=%d count=%d host=%s", group.index, len(group.m), node.String())

	return nil
}

// MarshalJSON is a custom marshaller for an affinity group
func (group *affinityGroup) MarshalJSON() ([]byte, error) {
	g := struct {
		ID    string
		Index int
		Nodes []Node
	}{
		ID:    hex.EncodeToString(group.id),
		Index: group.index,
	}

	group.mu.RLock()
	defer group.mu.RUnlock()

	g.Nodes = make([]Node, 0, len(group.m))
	var i int
	for _, n := range group.m {
		g.Nodes = append(g.Nodes, *n)
		i++
	}

	return json.Marshal(g)
}

func (group *affinityGroup) checkNodes() {
	group.mu.Lock()
	for k, v := range group.m {
		rtt := group.trans.Ping(v)
		if rtt != 0 {
			v.RTT = rtt
			v.Heartbeats++
			v.LastSeen = time.Now()
			group.m[k] = v
		}
	}
	group.mu.Unlock()
}
