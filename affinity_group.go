package kelips

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

var (
	errNodeNotFound = errors.New("node not found")
)

// AffinityGroup implements an affinity group interface to abstract local and retmote
// groups
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
