package kelips

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"log"
	"net"
	"strconv"
	"sync"
)

var (
	errKeyNotFound = errors.New("key not found")
)

// TupleHost is the host port of a key tuple
type TupleHost []byte

// NewTupleFromIPPort creates a new tuple with an ip an port
func NewTupleFromIPPort(ip net.IP, port int) TupleHost {
	pb := make([]byte, 2)
	binary.BigEndian.PutUint16(pb, uint16(port))
	return TupleHost(append(ip, pb...))
}

// NewTupleHostFromHostPort creates a tuple from a ip string and port
func NewTupleHostFromHostPort(host string, port int) TupleHost {
	ip := net.ParseIP(host)
	return NewTupleFromIPPort(ip, port)
}

// NewTupleHost creates a tuple from a host:port string
func NewTupleHost(hostport string) TupleHost {
	host, pstr, _ := net.SplitHostPort(hostport)

	port, _ := strconv.ParseUint(pstr, 10, 16)
	return NewTupleHostFromHostPort(host, int(port))
}

// ID returns the hash of the host port bytes
func (host TupleHost) ID(h hash.Hash) []byte {
	h.Write(host)
	sh := h.Sum(nil)
	return sh[:]
}

// IPAddress returns the v4 or v6 address
func (host TupleHost) IPAddress() net.IP {
	return net.IP(host[:len(host)-2])
}

// Port returns the port number
func (host TupleHost) Port() uint16 {
	return binary.BigEndian.Uint16(host[len(host)-2:])
}

func (host TupleHost) String() string {
	return host.IPAddress().String() + fmt.Sprintf(":%d", host.Port())
}

// Copy returns a copy of the tuple host
func (host TupleHost) Copy() TupleHost {
	out := make(TupleHost, len(host))
	copy(out, host)
	return out
}

// InmemTuples implements an in-memory  TupleStore
type InmemTuples struct {
	mu sync.RWMutex
	m  map[string][]TupleHost
}

// NewInmemTuples instantiates an in-memory tuple store
func NewInmemTuples() *InmemTuples {
	return &InmemTuples{m: make(map[string][]TupleHost)}
}

// Iter iterates over all the tuples.  If the callback returns false, iteration
// is terminated
func (ft *InmemTuples) Iter(f func(key []byte, hosts []TupleHost) bool) {
	ft.mu.RLock()
	for k, v := range ft.m {
		if !f([]byte(k), v) {
			break
		}
	}
	ft.mu.RUnlock()
}

// Count returns the total number of keys in the store
func (ft *InmemTuples) Count() int {
	ft.mu.RLock()
	c := len(ft.m)
	ft.mu.RUnlock()

	return c
}

// Insert adds a new host for a name if it does not already exist.  It returns true
// if the host was added
func (ft *InmemTuples) Insert(key []byte, h TupleHost) error {
	name := string(key)

	ft.mu.Lock()
	defer ft.mu.Unlock()

	hosts, ok := ft.m[name]
	if !ok {
		ft.m[name] = []TupleHost{h}
		log.Printf("[DEBUG] Tuple added key=%q host=%s", name, h)
		return nil
	}

	// Check if we already have the host
	for _, v := range hosts {
		if h.String() == v.String() {
			return nil
		}
	}

	ft.m[name] = append(hosts, h)
	log.Printf("[DEBUG] Tuple added key=%x host=%s", name, h)
	return nil
}

// Delete deletes a key removing all associated TupleHosts
func (ft *InmemTuples) Delete(key []byte) error {
	k := string(key)

	ft.mu.Lock()
	if _, ok := ft.m[k]; ok {
		delete(ft.m, k)
		ft.mu.Unlock()
		return nil
	}
	ft.mu.Unlock()
	return errKeyNotFound
}

// Get returns a list of hosts for a key.  It returns nil if the name is not
// found
func (ft *InmemTuples) Get(key []byte) ([]TupleHost, error) {
	name := string(key)

	ft.mu.RLock()
	defer ft.mu.RUnlock()

	if hosts, ok := ft.m[name]; ok {
		return hosts, nil
	}

	return nil, errKeyNotFound
}

// DeleteKeyHost deletes a host associated to the name returning true if it was deleted
func (ft *InmemTuples) DeleteKeyHost(key []byte, h TupleHost) bool {
	name := string(key)

	ft.mu.Lock()
	defer ft.mu.Unlock()

	hosts, ok := ft.m[name]
	if !ok {
		return false
	}

	for i, v := range hosts {
		if h.String() == v.String() {
			ft.m[name] = append(hosts[:i], hosts[i+1:]...)

			log.Printf("[DEBUG] Tuple deleted key=%q host=%s", key, h)
			return true
		}
	}

	return false
}

// ExpireHost removes a host from all keys referring to it
func (ft *InmemTuples) ExpireHost(tuple TupleHost) bool {
	var ok bool
	th := tuple.String()

	ft.mu.Lock()
	for k, hosts := range ft.m {
		for i, h := range hosts {
			if h.String() == th {
				ft.m[k] = append(hosts[:i], hosts[i+1:]...)
				log.Printf("[DEBUG] Tuple expired key=%q host=%s", k, th)
				ok = true
				break
			}
		}
	}
	ft.mu.Unlock()

	return ok
}
