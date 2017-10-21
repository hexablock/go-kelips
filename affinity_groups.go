package kelips

import (
	"bytes"
	"math/big"
)

type affinityGroups []*affinityGroup

// get the affinity group for the given hash id.  This uses a binary tree search
// to find the correct affinity group
func (ct affinityGroups) get(id []byte) *affinityGroup {
	var (
		arr = ct
		l   = len(arr)
		i   int
	)

REPEAT_ON_HALF:
	i = len(arr) / 2
	if i != 0 && i != l {

		s := bytes.Compare(id, arr[i].id)
		if s < 0 {
			arr = arr[:i]
			goto REPEAT_ON_HALF
		} else if s > 0 {
			arr = arr[i:]
			goto REPEAT_ON_HALF
		}

	}

	return arr[i]
}

func genAffinityGroups(numGroups int64, hashSize int64) affinityGroups {
	// Calculate the size of the keyspace
	var keyspace big.Int
	keyspace.Exp(big.NewInt(2), big.NewInt(hashSize*8), nil)
	// Number of affinity groups
	k := big.NewInt(numGroups)
	// Size of each group given the keyspace
	groupSize := new(big.Int).Div(&keyspace, k)

	ags := make([]*affinityGroup, numGroups)
	// First group i.e. 0 group
	ags[0] = newAffinityGroup(make([]byte, hashSize), 0)
	// Generate the remainder groups
	for i := int64(1); i < numGroups; i++ {
		gi := new(big.Int).Mul(big.NewInt(i), groupSize)
		ags[i] = newAffinityGroup(gi.Bytes(), int(i))
	}

	return affinityGroups(ags)
}
