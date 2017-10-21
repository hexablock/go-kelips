package kelips

import (
	"crypto/sha256"
	"hash"
)

// Config is the Kelips configuration
type Config struct {
	Hostname string

	// Number of affinity groups. Optimally sqrt(n) where n is the number of nodes
	NumAffinityGroups int

	// Hash function generator default: sha256
	HashFunc func() hash.Hash
}

// DefaultConfig returns a default sane config
func DefaultConfig() *Config {
	conf := &Config{
		NumAffinityGroups: 2,
		HashFunc: func() hash.Hash {
			return sha256.New()
		},
	}

	return conf
}
