package kelips

import (
	"testing"
)

func Test_Client(t *testing.T) {

	c1 := fastTestConf("127.0.0.1:54940")
	t1 := newBareTrans("127.0.0.1:54940")
	k1 := Create(c1, t1)

	testkey := []byte("key")
	testkey1 := []byte("test-key-test")
	client, err := NewClient("127.0.0.1:54940")
	if err != nil {
		t.Fatal(err)
	}

	if err = client.Insert(testkey, NewTupleHostFromHostPort("127.0.0.1", 54940)); err != nil {
		t.Fatal(err)
	}
	if err = client.Insert(testkey1, NewTupleHostFromHostPort("127.0.0.1", 54940)); err != nil {
		t.Fatal(err)
	}

	lns, err := client.LookupNodes(testkey1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(lns) < 2 {
		t.Fatal("didn't get enough nodes")
	}

	n1, err := client.Lookup(testkey)
	if err != nil {
		t.Error(err)
	}
	if len(n1) < 1 {
		t.Error("no nodes returned")
	}

	n1, err = client.Lookup(testkey1)
	if err != nil {
		t.Error(err)
	}
	if len(n1) < 1 {
		t.Error("no nodes returned")
	}

	_, err = client.LookupGroupNodes(testkey)
	if err == nil {
		t.Fatal("should fail")
	}

	if err = client.Delete(testkey, NewTupleHostFromHostPort("127.0.0.1", 54940)); err != nil {
		t.Fatal(err)
	}
	_, err = client.Lookup(testkey)
	if err == nil {
		t.Fatal("should fail")
	}

	k1.Snapshot()

}
