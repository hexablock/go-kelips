package kelips

import (
	"errors"
	"fmt"
	"hash"
	"log"
	"sync"
	"time"

	"github.com/hexablock/hexatype"
	"github.com/hexablock/vivaldi"
)

var (
	errNodeExists = errors.New("node exists")
)

type propReq struct {
	typ   int
	key   []byte
	tuple TupleHost
}

type localGroup struct {
	local *hexatype.Node

	// Local group index
	idx int

	// hash function
	hashFunc func() hash.Hash

	// Group tuples
	tuples TupleStore

	// All groups
	groups affinityGroups

	// propogation request channel
	propReqs chan *propReq

	// Network transport
	trans Transport
}

func (lrpc *localGroup) Delete(key []byte, tuple TupleHost, propogate bool) error {
	ok := lrpc.tuples.DeleteKeyHost(key, tuple)
	if ok && propogate {
		prop := &propReq{
			typ:   0,
			key:   make([]byte, len(key)),
			tuple: make([]byte, len(tuple)),
		}
		copy(prop.key, key)
		copy(prop.tuple, tuple)

		lrpc.propReqs <- prop
	}
	return nil
}

func (lrpc *localGroup) Insert(key []byte, tuple TupleHost, propogate bool) error {
	err := lrpc.tuples.Insert(key, tuple)
	if err == nil && propogate {
		prop := &propReq{
			typ:   1,
			key:   make([]byte, len(key)),
			tuple: make([]byte, len(tuple)),
		}
		copy(prop.key, key)
		copy(prop.tuple, tuple)

		lrpc.propReqs <- prop
	}

	return err
}

func (lrpc *localGroup) propogateDelete(key []byte, tuple TupleHost, nodes []hexatype.Node) {
	var err error
	for _, node := range nodes {
		if node.Host() == lrpc.local.Host() {
			continue
		}
		if err = lrpc.trans.Delete(node.Host(), key, tuple, false); err != nil {
			log.Printf("[ERROR] Failed to propogate: %s host=%s key=%x", err, node.Host(), key)
		}
	}
}

func (lrpc *localGroup) propogateInsert(key []byte, tuple TupleHost, nodes []hexatype.Node) {
	for _, node := range nodes {
		if node.Host() == lrpc.local.Host() {
			continue
		}
		if err := lrpc.trans.Insert(node.Host(), key, tuple, false); err != nil {
			log.Printf("[ERROR] Failed to propogate insert: %v host=%s key=%x",
				err, node.Host(), key)
		}
	}
}

// propogate inserts and deletes to the remaining group nodes
func (lrpc *localGroup) propogate(hashFunc func() hash.Hash) {
	h := hashFunc()

	for prop := range lrpc.propReqs {

		h.Reset()
		h.Write(prop.key)
		id := h.Sum(nil)

		group := lrpc.groups.get(id)
		nodes := group.Nodes()

		switch prop.typ {
		case 0:
			lrpc.propogateDelete(prop.key, prop.tuple, nodes)
		case 1:
			lrpc.propogateInsert(prop.key, prop.tuple, nodes)
		default:
			// unrecognized
			log.Printf("[ERROR] Unrecognized propogation request: %d", prop.typ)
			continue
		}

	}
}

func (lrpc *localGroup) LookupNodes(key []byte, min int) ([]*hexatype.Node, error) {
	h := lrpc.hashFunc()
	h.Write(key)
	sh := h.Sum(nil)

	group := lrpc.groups.get(sh)
	nodes := group.Nodes()

GET_MORE:
	if len(nodes) >= min {
		out := make([]*hexatype.Node, len(nodes))
		for i := range nodes {
			out[i] = &nodes[i]
		}
		return out, nil
	}

	group = lrpc.groups.nextClosestGroup(group)
	if group == nil {
		return nil, fmt.Errorf("nodes not found: %x", key)
	}

	nodes = append(nodes, group.Nodes()...)
	goto GET_MORE

}

func (lrpc *localGroup) LookupGroupNodes(key []byte) ([]*hexatype.Node, error) {
	h := lrpc.hashFunc()
	h.Write(key)
	sh := h.Sum(nil)

	group := lrpc.groups.get(sh)
	n := group.Nodes()
	nodes := make([]*hexatype.Node, len(n))
	for i := range n {
		nodes[i] = &n[i]
	}
	return nodes, nil
}

func (lrpc *localGroup) Lookup(key []byte) ([]*hexatype.Node, error) {
	tuples, err := lrpc.tuples.Get(key)
	if err != nil {
		return nil, err
	}

	nodes := make([]*hexatype.Node, 0, len(tuples))
	h := lrpc.hashFunc()
	for _, tuple := range tuples {
		h.Reset()
		id := tuple.ID(h)
		group := lrpc.groups.get(id)
		if node, ok := group.getNode(tuple.String()); ok {
			nodes = append(nodes, node)
		}
	}

	return nodes, nil
}

func (lrpc *localGroup) Snapshot() *Snapshot {
	snapshot := &Snapshot{
		Tuples: make([]*Tuple, 0, lrpc.tuples.Count()),
		Nodes:  make([]*hexatype.Node, 0, lrpc.groups.nodeCount()),
	}

	// handle all tuples
	lrpc.tuples.Iter(func(key []byte, hosts []TupleHost) bool {
		tuple := &Tuple{Key: key, Hosts: make([][]byte, 0, len(hosts))}
		for _, h := range hosts {
			tuple.Hosts = append(tuple.Hosts, h)
		}
		snapshot.Tuples = append(snapshot.Tuples, tuple)
		return true
	})

	lrpc.groups.iterNodes(func(node hexatype.Node) bool {
		snapshot.Nodes = append(snapshot.Nodes, &node)
		return true
	})

	return snapshot
}

// affinityGroup is a partial view of the nodes part of a given affinity group
type affinityGroup struct {
	// id constructed dividing the hash keyspace by NumAffinityGroups
	id []byte

	// k value of this group
	index int

	// Nodes part of the affinity group
	mu sync.RWMutex
	m  map[string]*hexatype.Node
}

func newAffinityGroup(id []byte, index int) *affinityGroup {
	return &affinityGroup{
		id:    id,
		index: index,
		m:     make(map[string]*hexatype.Node),
	}
}

func (group *affinityGroup) count() int {
	group.mu.RLock()
	defer group.mu.RUnlock()

	return len(group.m)
}

func (group *affinityGroup) Nodes() []hexatype.Node {
	group.mu.RLock()
	n := make([]hexatype.Node, 0, len(group.m))
	for _, node := range group.m {
		n = append(n, *node)
	}
	group.mu.RUnlock()

	return n
}

func (group *affinityGroup) getNode(hostname string) (*hexatype.Node, bool) {
	group.mu.RLock()
	defer group.mu.RUnlock()

	n, ok := group.m[hostname]
	if ok {
		return n, ok
	}
	return nil, false
}

// pingNode updates the heartbeat count, rtt, and last seen values
func (group *affinityGroup) pingNode(hostname string, coord *vivaldi.Coordinate, rtt time.Duration) error {
	group.mu.Lock()
	defer group.mu.Unlock()

	//group.mu.RLock()
	node, ok := group.m[hostname]
	if !ok {
		//group.mu.RUnlock()
		return fmt.Errorf("node not found: %s", hostname)
	}
	//group.mu.RUnlock()

	//group.mu.Lock()

	node.Heartbeats++
	node.LastSeen = time.Now().UnixNano()
	node.Coordinates = coord
	//group.m[hostname] = node

	//group.mu.Unlock()

	//log.Println("[DEBUG] Pinged", hostname, rtt)

	return nil
}

func (group *affinityGroup) removeNode(hostname string) error {
	group.mu.RLock()
	_, ok := group.m[hostname]
	if !ok {
		group.mu.RUnlock()
		return fmt.Errorf("node not found: %s", hostname)
	}
	group.mu.RUnlock()

	group.mu.Lock()
	delete(group.m, hostname)
	group.mu.Unlock()

	log.Printf("[INFO] Node removed group=%d count=%d node=%s", group.index,
		len(group.m), hostname)

	return nil
}

func (group *affinityGroup) addNode(node *hexatype.Node, force bool) error {

	group.mu.RLock()
	if _, ok := group.m[node.Host()]; ok && !force {
		group.mu.RUnlock()
		return errNodeExists
	}
	group.mu.RUnlock()

	group.mu.Lock()
	node.LastSeen = time.Now().UnixNano()
	group.m[node.Host()] = node
	group.mu.Unlock()

	log.Printf("[INFO] Node added group=%d count=%d host=%s", group.index, len(group.m), node.Host())

	return nil
}

// MarshalJSON is a custom marshaller for an affinity group
// func (group *affinityGroup) MarshalJSON() ([]byte, error) {
// 	g := struct {
// 		ID    string
// 		Index int
// 		Nodes []Host
// 	}{
// 		ID:    hex.EncodeToString(group.id),
// 		Index: group.index,
// 	}
//
// 	group.mu.RLock()
// 	defer group.mu.RUnlock()
//
// 	g.Nodes = make([]Host, 0, len(group.m))
// 	var i int
// 	for _, n := range group.m {
// 		g.Nodes = append(g.Nodes, *n)
// 		i++
// 	}
//
// 	return json.Marshal(g)
// }
//

// localhost is the local host to skip
// func (group *affinityGroup) checkNodes(localhost string) {
// 	nodes := group.Nodes()
// 	for i, n := range nodes {
// 		if n.Host() == localhost {
// 			// Update self coordinates
// 			group.pingNode(n.Host(), group.kelips.coordClient.GetCoordinate(), 0)
// 			nodes = append(nodes[:i], nodes[i+1:]...)
// 			break
// 		}
// 	}
//
// 	for _, n := range nodes {
// 		coord, rtt, err := group.kelips.trans.Ping(n.Host())
// 		if err != nil {
// 			log.Println("[ERROR] Ping failed", n.Host(), err)
// 			continue
// 		}
//
// 		if rtt == 0 {
// 			continue
// 		}
//
// 		if err = group.pingNode(n.Host(), coord, rtt); err != nil {
// 			log.Println("[ERROR] Ping failed", n, err)
// 			continue
// 		}
//
// 		if _, err = group.kelips.coordClient.Update(n.Host(), coord, rtt); err != nil {
// 			log.Println("[ERROR] Update coord failed", n, err)
// 		}
// 	}
//
// }
