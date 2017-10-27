package kelips

import (
	"crypto/sha256"
	"net"
	"testing"
)

func Test_Node(t *testing.T) {

	n := &Node{Host: &Host{Addr: net.ParseIP("127.0.0.1"), Port: 10000}}
	n.init(sha256.New())
	n.MarshalJSON()
	if n.ID == nil {
		t.Fatal("id nil")
	}

	if n.String() != "127.0.0.1:10000" {
		t.Fatal("wrong hostname", n.String())
	}
	if n.String() != n.String() {
		t.Fatal("wrong hostname", n.String())
	}

	b := n.Bytes()

	h1 := NewHostFromBytes(b)
	if n.String() != h1.String() {
		t.Fatal(n.String(), h1.String())
	}
}
