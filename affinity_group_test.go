package kelips

import (
	"bytes"
	"crypto/sha256"
	"testing"
	"time"
)

func Test_AffinityGroup(t *testing.T) {
	var k int64 = 48
	groups := genAffinityGroups(k, 32)
	if int64(len(groups)) != k {
		t.Fatalf("groups have=%d want=%d", len(groups), k)
	}

	zero := make([]byte, 32)
	for i, v := range groups[1:] {
		if v.ID() == nil {
			t.Error("id should not be nil")
		}
		if bytes.Compare(v.id, zero) == 0 {
			t.Fatal("should not be zero")
		}
		if v.Index() != (i + 1) {
			t.Error("wrong index")
		}
	}
	h := sha256.New()
	h.Write([]byte("foo"))
	sh := h.Sum(nil)
	g := groups.get(sh[:])

	n := &Node{Host: NewHost("127.0.0.1", 23232)}
	n.init(sha256.New)

	err := g.addNode(n)
	if err != nil {
		t.Fatal(err)
	}
	if err = g.addNode(n); err == nil {
		t.Fatal("should fail")
	}
	g.MarshalJSON()

	if _, ok := g.m["127.0.0.1:23232"]; !ok {
		t.Fatal("should have node")
	}

	rtt := 20 * time.Millisecond
	if err = g.pingNode("127.0.0.1:23232", rtt); err != nil {
		t.Fatal(err)
	}

	nd, _ := g.m["127.0.0.1:23232"]
	if nd.Heartbeats != 2 {
		t.Fatal("heartbeat should be 2", nd.Heartbeats, nd.RTT)
	}
	if nd.RTT != rtt {
		t.Fatal("rtt should be", rtt)
	}

	if err = g.pingNode("ffobar", time.Duration(5000)); err == nil {
		t.Fatal("should fail")
	}

	if err = g.removeNode("127.0.0.1:23232"); err != nil {
		t.Fatal(err)
	}
	if err = g.removeNode("ffoobar"); err == nil {
		t.Fatal("should fail")
	}
}
