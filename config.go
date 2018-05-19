package kelips

import (
	"crypto/sha256"
	"hash"
)

// Config holds the kelips config to initialize the dht
type Config struct {
	// AdvertiseHost used to compute the hash.  This may be different from the
	// transport address
	AdvertiseHost string

	// Number of affinity groups
	K int

	// Setting this to true will cause writes to be propogated to all nodes in
	// in the group.  The default is false relying on the underlying gossip
	// transport to provide propogation
	EnablePropogation bool

	// Hash function to use
	HashFunc func() hash.Hash

	// Tuple store. Defaults in an in-mem one if not specified
	TupleStore TupleStore

	Region string
	Sector string
	Zone   string

	// Meta is serialized to binary and made part of the node object
	Meta map[string]string
}

// DefaultConfig returns a minimum required config
func DefaultConfig(host string) *Config {
	return &Config{
		AdvertiseHost: host,
		K:             2,
		HashFunc:      sha256.New,
		Region:        "global",
		Sector:        "sector1",
		Zone:          "zone1",
		Meta:          make(map[string]string),
	}
}
