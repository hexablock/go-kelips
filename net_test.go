package kelips

import (
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/hexablock/hexatype"
	"github.com/hexablock/vivaldi"
)

type MockAffinityGroupRPC struct {
	mu    sync.Mutex
	hosts map[string][]TupleHost
}

// This is called to set rrt on the local group for the host
func (group *MockAffinityGroupRPC) Ping() *vivaldi.Coordinate {
	return &vivaldi.Coordinate{}
}

// Lookup nodes from the local view
func (group *MockAffinityGroupRPC) Lookup(key []byte) ([]*hexatype.Node, error) {
	group.mu.Lock()
	defer group.mu.Unlock()
	if hosts, ok := group.hosts[string(key)]; ok {
		out := make([]*hexatype.Node, 0, len(hosts))
		for _, host := range hosts {
			//out = append(out, NewNodeFromTuple(host))
			out = append(out, &hexatype.Node{Address: host})
		}
		return out, nil
	}
	return nil, fmt.Errorf("key not found: %s", key)
}

func (group *MockAffinityGroupRPC) LookupGroupNodes(key []byte) ([]*hexatype.Node, error) {
	group.mu.Lock()
	defer group.mu.Unlock()

	if hosts, ok := group.hosts[string(key)]; ok {
		out := make([]*hexatype.Node, 0, len(hosts))
		for _, host := range hosts {
			//out = append(out, NewNodeFromTuple(host))
			out = append(out, &hexatype.Node{Address: host})
		}
		return out, nil
	}
	return nil, fmt.Errorf("key not found: %s", key)
}

// Insert to local group
func (group *MockAffinityGroupRPC) Insert(key []byte, host TupleHost, prop bool) error {
	group.mu.Lock()
	defer group.mu.Unlock()

	val, ok := group.hosts[string(key)]
	if ok {
		group.hosts[string(key)] = append(val, host)

	} else {
		group.hosts[string(key)] = []TupleHost{host}
	}

	return nil
}

// Delete from local group
func (group *MockAffinityGroupRPC) Delete(key []byte, tuple TupleHost, prop bool) error {
	group.mu.Lock()
	defer group.mu.Unlock()

	if _, ok := group.hosts[string(key)]; ok {
		delete(group.hosts, string(key))
	}
	return nil
}

func newBareTrans(addr string) *UDPTransport {

	laddr, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		panic(err)
	}

	ln, err := net.ListenUDP("udp4", laddr)
	if err != nil {
		panic(err)
	}

	return &UDPTransport{conn: ln}

}

func newTestTransport(addr string) *UDPTransport {
	trans := newBareTrans(addr)
	group := &MockAffinityGroupRPC{hosts: make(map[string][]TupleHost)}
	trans.Register(group)
	return trans
}

func Test_UDPTransport(t *testing.T) {
	t1 := newTestTransport("127.0.0.1:23456")
	t2 := newTestTransport("127.0.0.1:23457")
	t3 := newTestTransport("127.0.0.1:23458")

	if err := t1.Insert("127.0.0.1:23457", []byte("key"), NewTupleHostFromHostPort("127.0.0.1", 23456), false); err != nil {
		t.Fatal(err)
	}
	if err := t2.Insert("127.0.0.1:23458", []byte("key"), NewTupleHostFromHostPort("127.0.0.1", 23456), false); err != nil {
		t.Fatal(err)
	}
	if err := t3.Insert("127.0.0.1:23456", []byte("key"), NewTupleHostFromHostPort("127.0.0.1", 23456), false); err != nil {
		t.Fatal(err)
	}

	if hosts, err := t1.Lookup("127.0.0.1:23457", []byte("key")); err != nil || hosts == nil || len(hosts) == 0 {
		t.Fatal("should have hosts", err)
	}
	if hosts, err := t2.Lookup("127.0.0.1:23458", []byte("key")); err != nil || hosts == nil || len(hosts) == 0 {
		t.Fatal("should have hosts", err)
	}
	hosts, err := t3.Lookup("127.0.0.1:23456", []byte("key"))
	if err != nil || hosts == nil || len(hosts) == 0 {
		t.Fatal("should have hosts", err)
	}
	if hosts[0].Port() != 23456 {
		t.Fatal("wrong host", hosts[0].Host())
	}

	if err = t2.Delete("127.0.0.1:23457", []byte("key"), NewTupleHostFromHostPort("127.0.0.1", 23456), false); err != nil {
		t.Fatal(err)
	}

	nodes, err := t2.Lookup("127.0.0.1:23457", []byte("key"))
	if err == nil && len(nodes) != 0 {
		t.Fatal("should fail", nodes)
	}

	nodes, err = t3.LookupGroupNodes("127.0.0.1:23457", []byte("key"))
	if err == nil && len(nodes) != 0 {
		t.Fatal("should fail", nodes)
	}

	// _, rtt1, err := t1.Ping("127.0.0.1:23457")
	// if err != nil {
	// 	t.Error("err")
	// }
	// if rtt1 <= 0 {
	// 	t.Error("0 rtt")
	// }
	//
	// _, rtt2, err := t2.Ping("127.0.0.1:23458")
	// if err != nil {
	// 	t.Error("err")
	// }
	// if rtt2 <= 0 {
	// 	t.Error("0 rtt")
	// }
	//
	// _, rtt3, err := t3.Ping("127.0.0.1:23456")
	// if err != nil {
	// 	t.Error("err")
	// }
	// if rtt3 <= 0 {
	// 	t.Error("0 rtt")
	// }
}
