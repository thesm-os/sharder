// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package partitiontest_test

import (
	"hash/fnv"
	"testing"

	"go.thesmos.sh/sharder/partition/partitiontest"
)

// fnv1aVersion is the version of the hash fnv1a.
const fnv1aVersion = 1

// fnv1a is the hash FNV-1a of 64 bits at version fnv1aVersion, as an application
// registers a hash of its own. It keeps every law of partition.Hash.
type fnv1a struct{}

// Sum64 returns FNV-1a of 64 bits of key.
func (fnv1a) Sum64(key []byte) uint64 {
	h := fnv.New64a()
	// The Write of a hash.Hash never returns an error.
	_, _ = h.Write(key)
	return h.Sum64()
}

// Version returns fnv1aVersion.
func (fnv1a) Version() uint32 {
	return fnv1aVersion
}

// TestHash checks that fnv1a passes the checks of the suite of partition.Hash.
func TestHash(t *testing.T) {
	t.Parallel()

	partitiontest.RunHashSuite(t, func() fnv1a { return fnv1a{} })
}
