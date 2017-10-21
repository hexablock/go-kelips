package kelips

import (
	"crypto/sha256"
	"hash"
	"net"
	"testing"
)

func Test_Node(t *testing.T) {

	n := &Node{Host: &Host{Addr: net.ParseIP("127.0.0.1"), Port: 10000}}
	n.init(func() hash.Hash {
		return sha256.New()
	})
	n.MarshalJSON()
	if n.ID == nil {
		t.Fatal("id nil")
	}

	if n.Hostname() != "127.0.0.1:10000" {
		t.Fatal("wrong hostname", n.Hostname())
	}
	if n.String() != n.Hostname() {
		t.Fatal("wrong hostname", n.String())
	}
}
