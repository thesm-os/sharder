// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package partitiontest

import (
	"math"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/partition"
)

// Vector is a key and the sum that a hash function returns for it.
type Vector struct {
	// Key is the key.
	Key []byte
	// Sum is the hash of Key.
	Sum uint64
}

// AssertHash checks the laws of partition.Hash against h, and fails t when h breaks
// one. Version of h must return version, and Sum64 of h must return the Sum of each
// vector for its Key. AssertHash also fails t when vectors is empty, because every
// hash states vectors of its own. It runs each law as a parallel subtest of t. The
// subtest of a law has the name of the method that the law constrains.
func AssertHash(t *testing.T, h partition.Hash, version uint32, vectors ...Vector) {
	t.Helper()

	t.Run("Sum64", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the sum of each vector", func(t *testing.T) {
			t.Parallel()
			assert.NotEmpty(t, vectors, "the hash must state at least one vector")
			for _, v := range vectors {
				expect.Equal(t, h.Sum64(v.Key), v.Sum, "Sum64 must return the sum of the vector")
			}
		})

		t.Run("returns the same hash for the same key", func(t *testing.T) {
			t.Parallel()
			prop.Deterministic(t, func(key []byte) (uint64, error) { return h.Sum64(key), nil },
				"Sum64 must return the same hash for the same key")
		})
	})

	t.Run("Version", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the version of the hash", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, h.Version(), version, "Version must return the version of the hash")
		})

		t.Run("returns a version of at least one", func(t *testing.T) {
			t.Parallel()
			assert.InRange(t, h.Version(), 1, math.MaxUint32, "Version must return a version of at least one")
		})
	})
}
