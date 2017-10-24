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

func newTestKelips(host string, port int, joins ...string) (*Kelips, error) {

	conf := DefaultConfig()
	conf.NumAffinityGroups = 2
	conf.Hostname = fmt.Sprintf("%s:%d", host, port)
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

	kelps, err := NewKelips(conf, serfConf)
	if err != nil {
		return nil, err
	}

	if len(joins) > 0 {
		err = kelps.Join(joins...)
	}

	return kelps, err
}

func Test_Kelips_Shutdown(t *testing.T) {
	peers := []string{"127.0.0.1:23456", "127.0.0.1:23457"}

	k0, err := newTestKelips("127.0.0.1", 23456)
	if err != nil {
		t.Fatal(err)
	}

	k1, err := newTestKelips("127.0.0.1", 23457, peers...)
	if err != nil {
		t.Fatal(err)
	}

	k2, err := newTestKelips("127.0.0.1", 23458, peers...)
	if err != nil {
		t.Fatal(err)
	}

	k3, err := newTestKelips("127.0.0.1", 23459, peers...)
	if err != nil {
		t.Fatal(err)
	}

	if k2.Serf() == nil {
		t.Fatal("serf should be set")
	}

	if err = k0.serf.UserEvent("k0", []byte("foobar"), false); err != nil {
		t.Error(err)
	}

	if err = k1.serf.UserEvent("k1", []byte("foobar"), false); err != nil {
		t.Error(err)
	}
	if err = k2.serf.UserEvent("k2", []byte("foobar"), false); err != nil {
		t.Error(err)
	}

	var params *serf.QueryParam
	if _, err = k1.serf.Query("127.0.0.1:23459", []byte{}, params); err != nil {
		t.Fatal(err)
	}

	<-time.After(2 * time.Second)

	if err = k0.Leave(); err != nil {
		t.Error(err)
	}
	k0.Shutdown()

	if err = k1.Leave(); err != nil {
		t.Error(err)
	}
	k1.Shutdown()

	if err = k2.Leave(); err != nil {
		t.Error(err)
	}
	k2.Shutdown()

	if err = k3.Leave(); err != nil {
		t.Error(err)
	}
	k3.Shutdown()

}

func Test_Kelips_Lookup(t *testing.T) {
	peers := []string{"127.0.0.1:33456", "127.0.0.1:33457"}

	k0, err := newTestKelips("127.0.0.1", 33456)
	if err != nil {
		t.Fatal(err)
	}

	k1, err := newTestKelips("127.0.0.1", 33457, peers...)
	if err != nil {
		t.Fatal(err)
	}

	k2, err := newTestKelips("127.0.0.1", 12458, peers...)
	if err != nil {
		t.Fatal(err)
	}

	k3, err := newTestKelips("127.0.0.1", 33459, peers...)
	if err != nil {
		t.Fatal(err)
	}

	<-time.After(1 * time.Second)

	n0 := k0.LookupGroup([]byte("127.0.0.1:33458")).Nodes()
	if len(n0) == 0 {
		t.Error("no nodes found")
	}

	n1 := k1.LookupGroup([]byte("127.0.0.1:33458")).Nodes()
	if len(n1) == 0 {
		t.Error("no nodes found")
	}

	n2 := k2.LookupGroup([]byte("127.0.0.1:33458")).Nodes()
	if len(n2) != len(n1) {
		t.Error("lookup length mismatch")
	}

	n3 := k3.LookupGroup([]byte("key")).Nodes()
	if len(n3) == 0 {
		t.Error("nothing for key")
	}

	// Insert
	if err = k0.Insert([]byte("key"), NewHost("127.0.0.1", 12458)); err != nil {
		t.Fatal(err)
	}

	<-time.After(100 * time.Millisecond)
	nodes, err := k2.Lookup([]byte("key"))
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
	l4, err := k0.Lookup([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if len(l4) != len(nodes) {
		t.Errorf("node count mismatch want=%d have=%d", len(nodes), len(l4))
	}

	// Leave
	if err = k0.Leave(); err != nil {
		t.Error(err)
	}
	k0.Shutdown()
	<-k0.serf.ShutdownCh()
	// Check graceful leave
	<-time.After(100 * time.Millisecond)
	nj := k2.LookupGroup([]byte("127.0.0.1:33458")).Nodes()
	if len(nj) >= len(n2) {
		t.Error("should have less than", len(nj))
	}

	// Force shutdown
	k1.Shutdown()
	<-k1.serf.ShutdownCh()

	// Check force shutdown
	<-time.After(1 * time.Second)
	nf := k2.LookupGroup([]byte("127.0.0.1:33458")).Nodes()
	if len(nf) != len(nj) {
		t.Fatal("should have same as leave")
	}

	if err = k2.Leave(); err != nil {
		t.Error(err)
	}
	k2.Shutdown()

	if err = k3.Leave(); err != nil {
		t.Error(err)
	}
	k3.Shutdown()

}
