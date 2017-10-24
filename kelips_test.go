package kelips

import (
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"
)

func newTestConfig() *serf.Config {
	config := serf.DefaultConfig()
	config.Init()

	// Set probe intervals that are aggressive for finding bad nodes
	config.MemberlistConfig.GossipInterval = 5 * time.Millisecond
	config.MemberlistConfig.ProbeInterval = 50 * time.Millisecond
	config.MemberlistConfig.ProbeTimeout = 25 * time.Millisecond
	config.MemberlistConfig.SuspicionMult = 1

	// Set a short reap interval so that it can run during the test
	config.ReapInterval = 1 * time.Second

	// Set a short reconnect interval so that it can run a lot during tests
	config.ReconnectInterval = 100 * time.Millisecond

	// Set basically zero on the reconnect/tombstone timeouts so that
	// they're removed on the first ReapInterval.
	config.ReconnectTimeout = 1 * time.Microsecond
	config.TombstoneTimeout = 1 * time.Microsecond

	return config
}

type testServer struct {
	trans *SerfTransport
	klp   *Kelips
}

func newTestServer(host string, port int, joins ...string) (*testServer, error) {

	conf := DefaultConfig(fmt.Sprintf("%s:%d", host, port))
	conf.NumAffinityGroups = 2
	conf.HeartbeatInterval = 1 * time.Second

	serfConf := newTestConfig()
	serfConf.NodeName = conf.Hostname
	serfConf.Tags = map[string]string{"tag": "tag-value"}
	serfConf.EnableNameConflictResolution = true
	serfConf.LogOutput = ioutil.Discard
	serfConf.MemberlistConfig.AdvertiseAddr = host
	serfConf.MemberlistConfig.AdvertisePort = port
	serfConf.MemberlistConfig.BindAddr = host
	serfConf.MemberlistConfig.BindPort = port
	serfConf.MemberlistConfig.LogOutput = ioutil.Discard

	srf, err := NewSerfTransport(serfConf)
	if err != nil {
		return nil, err
	}

	kelps, err := NewKelips(conf, srf)
	if err != nil {
		return nil, err
	}

	if len(joins) > 0 {
		err = srf.Join(joins...)
	}

	ts := &testServer{trans: srf, klp: kelps}

	return ts, err
}

func Test_Kelips_Shutdown(t *testing.T) {
	peers := []string{"127.0.0.1:23456", "127.0.0.1:23457"}

	k0, err := newTestServer("127.0.0.1", 23456)
	if err != nil {
		t.Fatal(err)
	}

	k1, err := newTestServer("127.0.0.1", 23457, peers...)
	if err != nil {
		t.Fatal(err)
	}

	k2, err := newTestServer("127.0.0.1", 23458, peers...)
	if err != nil {
		t.Fatal(err)
	}

	k3, err := newTestServer("127.0.0.1", 23459, peers...)
	if err != nil {
		t.Fatal(err)
	}

	if k2.trans.Serf() == nil {
		t.Fatal("serf should be set")
	}

	if err = k0.trans.serf.UserEvent("k0", []byte("foobar"), false); err != nil {
		t.Error(err)
	}

	if err = k1.trans.serf.UserEvent("k1", []byte("foobar"), false); err != nil {
		t.Error(err)
	}
	if err = k2.trans.serf.UserEvent("k2", []byte("foobar"), false); err != nil {
		t.Error(err)
	}

	var params *serf.QueryParam
	if _, err = k1.trans.serf.Query("127.0.0.1:23459", []byte{}, params); err != nil {
		t.Fatal(err)
	}

	<-time.After(2 * time.Second)

	if err = k0.trans.serf.Leave(); err != nil {
		t.Error(err)
	}
	k0.trans.serf.Shutdown()

	if err = k1.trans.serf.Leave(); err != nil {
		t.Error(err)
	}
	k1.trans.serf.Shutdown()

	if err = k2.trans.serf.Leave(); err != nil {
		t.Error(err)
	}
	k2.trans.serf.Shutdown()

	if err = k3.trans.serf.Leave(); err != nil {
		t.Error(err)
	}
	k3.trans.serf.Shutdown()

}

func Test_Kelips_Lookup(t *testing.T) {
	peers := []string{"127.0.0.1:33456", "127.0.0.1:33457"}

	k0, err := newTestServer("127.0.0.1", 33456)
	if err != nil {
		t.Fatal(err)
	}

	k1, err := newTestServer("127.0.0.1", 33457, peers...)
	if err != nil {
		t.Fatal(err)
	}

	k2, err := newTestServer("127.0.0.1", 12458, peers...)
	if err != nil {
		t.Fatal(err)
	}

	k3, err := newTestServer("127.0.0.1", 33459, peers...)
	if err != nil {
		t.Fatal(err)
	}

	<-time.After(1 * time.Second)

	n0 := k0.klp.LookupGroup([]byte("127.0.0.1:33458")).Nodes()
	if len(n0) == 0 {
		t.Error("no nodes found")
	}

	n1 := k1.klp.LookupGroup([]byte("127.0.0.1:33458")).Nodes()
	if len(n1) == 0 {
		t.Error("no nodes found")
	}

	n2 := k2.klp.LookupGroup([]byte("127.0.0.1:33458")).Nodes()
	if len(n2) != len(n1) {
		t.Error("lookup length mismatch")
	}

	n3 := k3.klp.LookupGroup([]byte("key")).Nodes()
	if len(n3) == 0 {
		t.Error("nothing for key")
	}

	// Insert
	if err = k0.klp.Insert([]byte("key"), NewHost("127.0.0.1", 12458)); err != nil {
		t.Fatal(err)
	}

	<-time.After(100 * time.Millisecond)
	nodes, err := k2.klp.Lookup([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) == 0 {
		t.Fatal("should have nodes")
	}
	var found bool
	for _, v := range nodes {
		if v.String() == "127.0.0.1:12458" {
			found = true
		}
	}
	if !found {
		t.Fatal("should be found")
	}

	// Forwarded lookup
	l4, err := k0.klp.Lookup([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if len(l4) != len(nodes) {
		t.Errorf("node count mismatch want=%d have=%d", len(nodes), len(l4))
	}

	// Leave
	if err = k0.trans.serf.Leave(); err != nil {
		t.Error(err)
	}
	k0.trans.serf.Shutdown()
	<-k0.trans.serf.ShutdownCh()
	// Check graceful leave
	<-time.After(100 * time.Millisecond)
	nj := k2.klp.LookupGroup([]byte("127.0.0.1:33458")).Nodes()
	if len(nj) >= len(n2) {
		t.Error("should have less than", len(nj))
	}

	// Force shutdown
	k1.trans.serf.Shutdown()
	<-k1.trans.serf.ShutdownCh()

	// Check force shutdown
	<-time.After(1 * time.Second)
	nf := k2.klp.LookupGroup([]byte("127.0.0.1:33458")).Nodes()
	if len(nf) != len(nj) {
		t.Fatal("should have same as leave")
	}

	if err = k2.trans.serf.Leave(); err != nil {
		t.Error(err)
	}
	k2.trans.serf.Shutdown()

	if err = k3.trans.serf.Leave(); err != nil {
		t.Error(err)
	}
	k3.trans.serf.Shutdown()

}
