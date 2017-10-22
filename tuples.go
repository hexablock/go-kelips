package kelips

import (
	"log"
	"sync"
)

// InmemTuples implements an in-memory  TupleStore
type InmemTuples struct {
	mu sync.RWMutex
	m  map[string][]*Host
}

// NewInmemTuples instantiates an in-memory tuple store
func NewInmemTuples() *InmemTuples {
	return &InmemTuples{m: make(map[string][]*Host)}
}

// Add adds a new host for a name if it does not already exist.  It returns true
// if the host was added
func (ft *InmemTuples) Add(name string, h *Host) bool {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	hosts, ok := ft.m[name]
	if !ok {
		ft.m[name] = []*Host{h}
		log.Printf("[INFO] Tuple added key=%s host=%s", name, h)
		return true
	}

	ok = false
	for _, v := range hosts {
		if h.String() == v.String() {
			ok = true
			break
		}
	}
	if !ok {
		ft.m[name] = append(hosts, h)
		log.Printf("[INFO] Tuple added key=%s host=%s", name, h)
		return true
	}
	return false
}

// Del deletes a host associated to the name returning true if it was deleted
func (ft *InmemTuples) Del(name string, h *Host) bool {
	ft.mu.RLock()
	hosts, ok := ft.m[name]
	if !ok {
		ft.mu.RUnlock()
		return false
	}
	//l := len(hosts)

	for i, v := range hosts {
		if h.String() == v.String() {
			ft.mu.RUnlock()

			ft.mu.Lock()
			ft.m[name] = append(hosts[:i], hosts[i+1:]...)
			ft.mu.Unlock()

			log.Printf("[INFO] Tuple deleted key=%s host=%s", name, h)
			return true
		}
	}
	ft.mu.RUnlock()
	return false
}

// Get returns a list of hosts for a name.  It returns nil if the name is not
// found
func (ft *InmemTuples) Get(name string) []*Host {
	ft.mu.RLock()
	defer ft.mu.RUnlock()

	hosts, _ := ft.m[name]
	return hosts
}

// ExpireHost removes a host from all keys referring to it
func (ft *InmemTuples) ExpireHost(host *Host) bool {
	var ok bool

	ft.mu.Lock()
	for k, hosts := range ft.m {
		for i, h := range hosts {
			if h.String() == host.String() {
				ft.m[k] = append(hosts[:i], hosts[i+1:]...)
				log.Printf("[INFO] Tuple expired key=%s host=%s", k, host)
				ok = true
				break
			}
		}
	}
	ft.mu.Unlock()

	return ok
}
