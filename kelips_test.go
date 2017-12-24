package kelips

import (
	"os"
	"testing"
	"time"

	"github.com/hexablock/log"
)

func TestMain(m *testing.M) {
	log.SetLevel("DEBUG")
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	os.Exit(m.Run())
}

func fastTestConf(addr string) *Config {
	c1 := DefaultConfig(addr)
	c1.Meta["host"] = addr
	c1.EnablePropogation = true
	//c1.PingMax = 300 * time.Millisecond
	//c1.PingMin = 100 * time.Millisecond
	return c1
}

func Test_Kelips(t *testing.T) {

	c1 := fastTestConf("127.0.0.1:54540")
	t1 := newBareTrans("127.0.0.1:54540")
	//	cc1, _ := vivaldi.NewClient(vivaldi.DefaultConfig())
	k1 := Create(c1, t1)

	c2 := fastTestConf("127.0.0.1:54541")
	t2 := newBareTrans("127.0.0.1:54541")
	//	cc2, _ := vivaldi.NewClient(vivaldi.DefaultConfig())
	k2 := Create(c2, t2)

	c3 := fastTestConf("127.0.0.1:54542")
	t3 := newBareTrans("127.0.0.1:54542")
	//	cc3, _ := vivaldi.NewClient(vivaldi.DefaultConfig())
	k3 := Create(c3, t3)

	testkey := []byte("key")
	testkey1 := []byte("test-key-test")

	if err := k1.Insert(testkey, NewTupleHostFromHostPort("127.0.0.1", 54540)); err != nil {
		t.Fatal(err)
	}
	if err := k2.Insert(testkey, NewTupleHostFromHostPort("127.0.0.1", 54541)); err != nil {
		t.Fatal(err)
	}
	if err := k3.Insert(testkey, NewTupleHostFromHostPort("127.0.0.1", 54542)); err != nil {
		t.Fatal(err)
	}

	if err := k1.Insert(testkey1, NewTupleHostFromHostPort("127.0.0.1", 54540)); err != nil {
		t.Fatal(err)
	}
	if err := k2.Insert(testkey1, NewTupleHostFromHostPort("127.0.0.1", 54541)); err != nil {
		t.Fatal(err)
	}
	if err := k3.Insert(testkey1, NewTupleHostFromHostPort("127.0.0.1", 54542)); err != nil {
		t.Fatal(err)
	}

	// Allow ping
	<-time.After(1 * time.Second)

	n1, err := k1.Lookup(testkey)
	t.Log(n1, err)
	n2, err := k2.Lookup(testkey)
	t.Log(n2, err)
	n3, err := k3.Lookup(testkey)
	t.Log(n3, err)

	n1, err = k1.Lookup(testkey1)
	t.Log(n1, err)
	n2, err = k2.Lookup(testkey1)
	t.Log(n2, err)
	n3, err = k3.Lookup(testkey1)
	t.Log(n3, err)

	if _, err = k2.Lookup([]byte("non-existent")); err == nil {
		t.Fatal("lookup should fail")
	}

	k1.Insert(testkey1, NewTupleHostFromHostPort("127.0.0.1", 54542))

	t.Log(k1.groups[0])
	t.Log(k1.groups[1])

	kn1 := k1.LocalNode()
	if kn1.Host() != "127.0.0.1:54540" {
		t.Fatal("wrong host", kn1.Host())
	}

	ss := k1.Snapshot()
	if len(ss.Nodes) != k1.local.groups.nodeCount() {
		t.Error("should have nodes", len(ss.Nodes), k1.local.groups.nodeCount())
	}

	if len(ss.Tuples) != k1.local.tuples.Count() {
		t.Error("should have tuples", len(ss.Tuples), k1.local.tuples.Count())
	}

	if int(ss.Groups) != k1.conf.NumGroups {
		t.Error("group mismatch")
	}

	nodes, err := k2.LookupNodes([]byte("foo"), 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) < 3 {
		t.Fatal("don't have enough nodes", len(nodes))
	}

	// b, _ := proto.Marshal(ss)
	// t.Log("Snapshot size", len(b))
	//
	// if err = k1.RemoveNode("127.0.0.1:54542"); err != nil {
	// 	t.Fatal(err)
	// }
	// if err = k3.RemoveNode("127.0.0.1:54548"); err == nil {
	// 	t.Fatal("should failed")
	// }

	// for _, g := range k1.groups {
	// 	n := g.Nodes()
	// 	//b, _ := json.MarshalIndent(n, "", "  ")
	// 	//t.Logf("%s\n", b)
	// 	b1, _ := msgpack.Marshal(n[0].Coordinates)
	// 	b2, _ := proto.Marshal(&n[0].Coordinates)
	// 	t.Logf("msgpack=%d protobuf=%d", len(b1), len(b2))
	// }

}
