// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package partitiontest

import (
	"math"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/partition/xxhash64"
)

// Sum64IsDeterministic checks that Sum64 returns the same hash for the same key.
//
//dokimi:check Hash.Sum64 defect=driftsSums
func Sum64IsDeterministic(h *HashHandle, c *prop.Case) {
	s := h.Subject()
	key := c.Draw(prop.Bytes(prop.MaxSize(maxKey)), labelKey)
	assert.Deterministic(h, func(key []byte) (uint64, error) { return s.Sum64(key), nil }, key, h.Statement())
}

// driftsSums returns a planted defect over xxhash64 whose Sum64 adds one to the hash
// after driftCalls calls. One goroutine runs a check, so the count of the calls has no
// lock.
func driftsSums(h *HashHandle) partition.Hash {
	calls := 0
	return NewHashStub(h, HashStubDelegateTo(xxhash64.Hash{}), WithHashSum64(func(key []byte) uint64 {
		calls++
		sum := xxhash64.Hash{}.Sum64(key)
		if calls > driftCalls {
			sum++
		}
		return sum
	}))
}

// VersionIsPositive checks that Version returns a version of at least one.
//
//dokimi:check Hash.Version defect=returnsVersionZero
func VersionIsPositive(h *HashHandle) {
	assert.InRange(h, h.Subject().Version(), 1, math.MaxUint32, h.Statement())
}

// returnsVersionZero returns a planted defect over xxhash64 whose Version returns 0.
func returnsVersionZero(h *HashHandle) partition.Hash {
	return NewHashStub(h, HashStubDelegateTo(xxhash64.Hash{}), WithHashVersion(func() uint32 { return 0 }))
}
