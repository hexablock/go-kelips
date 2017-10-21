package kelips

import (
	"log"
	"strings"

	"github.com/hashicorp/serf/serf"
)

// Kelips is the core kelips struct that maintains the internal state
type Kelips struct {
	// Local node information
	node Node

	// This nodes group index
	idx int

	// Kelips config
	conf *Config

	// Gossip config
	serfConf *serf.Config

	// Gossip transport
	serf *serf.Serf

	// All affinity groups in the dht
	groups affinityGroups

	// File tuples for this node
	tuples *filetuples

	// Events received via gossip used to to update state
	events chan serf.Event

	// Channel to block for completion
	stopped chan struct{}
}

// NewKelips initializes serf and starts the event handlers
func NewKelips(conf *Config, serfConf *serf.Config) (*Kelips, error) {
	mc := serfConf.MemberlistConfig
	k := &Kelips{
		node: Node{
			Host: NewHost(mc.AdvertiseAddr, uint16(mc.AdvertisePort)),
		},
		conf:     conf,
		serfConf: serfConf,
		events:   make(chan serf.Event, 64),
		stopped:  make(chan struct{}, 1),
		tuples:   newFiletuples(),
	}
	k.init()

	s, err := serf.Create(serfConf)
	if err != nil {
		return nil, err
	}

	k.serf = s

	go k.handleEvents()

	return k, nil
}

func (kelps *Kelips) init() {
	h := kelps.conf.HashFunc()
	k := int64(kelps.conf.NumAffinityGroups)
	// Init local node
	kelps.node.init(kelps.conf.HashFunc)
	// Set channel to receive events on
	kelps.serfConf.EventCh = kelps.events
	// Init affinity groups
	kelps.groups = genAffinityGroups(k, int64(h.Size()))
	// Set local group index
	group := kelps.groups.get(kelps.node.ID)
	kelps.idx = group.index

	log.Printf("[INFO] Kelips initialized group=%d id=%x total=%d",
		group.index, kelps.node.ID, len(kelps.groups))
}

// LookupGroup hashes a key and returning the associated group
func (kelps *Kelips) LookupGroup(key []byte) AffinityGroup {
	h := kelps.conf.HashFunc()
	h.Write(key)
	sh := h.Sum(nil)

	log.Printf("%x", sh)
	return kelps.groups.get(sh[:])
}

// Insert looks up the affinity group and inserts the key.  If the affinity os
// foreign it returns the affinity group and false otherwise it addes the key
// and returns the affinity group along with true
func (kelps *Kelips) Insert(key []byte) (group AffinityGroup, ok bool) {
	group = kelps.LookupGroup(key)

	nodes := group.Nodes()
	var n string
	for _, v := range nodes {
		n += v.Hostname() + " "
	}
	log.Printf("[DEBUG] Insertion candidates='%s'", strings.TrimSuffix(n, " "))

	if group.Index() != kelps.idx {
		return
	}

	kelps.tuples.add(string(key), kelps.node.Host)
	log.Printf("[DEBUG] Inserting key=%s host=%s", key, kelps.conf.Hostname)
	ok = true

	return
}

// Join issues a join to the gossip network
func (kelps *Kelips) Join(existing []string) error {
	_, err := kelps.serf.Join(existing, false)
	return err
}

// Serf returns the underlying serf instance i.e. the gossip transport
func (kelps *Kelips) Serf() *serf.Serf {
	return kelps.serf
}

// Leave instructs the node to leave the gossip network.  This ensures a clean
// shutdown
func (kelps *Kelips) Leave() error {
	return kelps.serf.Leave()
}

// Shutdown shutdowns down the dht.  This is a force shutdown as such Leave
// should be called before this for a clean shutdown.
func (kelps *Kelips) Shutdown() error {
	err := kelps.serf.Shutdown()
	//<-kelps.serf.ShutdownCh()

	//close(kelps.events)
	//<-kelps.stopped

	return err
}
