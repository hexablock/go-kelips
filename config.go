package kelips

import (
	"crypto/sha256"
	"hash"
	"time"
)

// Config is the Kelips configuration
type Config struct {
	// Hostname used to get the node id by the Hostname hash.  This must be the
	// transport advertise address
	Hostname string

	// Number of affinity groups. Optimally sqrt(n) where n is the number of nodes
	NumAffinityGroups int

	// Interval at which node rtt's are updated
	HeartbeatInterval time.Duration

	// Hash function generator default: sha256
	HashFunc func() hash.Hash
}

// DefaultConfig returns a default sane config
func DefaultConfig(hostname string) *Config {
	conf := &Config{
		Hostname:          hostname,
		NumAffinityGroups: 2,
		HeartbeatInterval: 30 * time.Second,
		HashFunc: func() hash.Hash {
			return sha256.New()
		},
	}

	return conf
}
