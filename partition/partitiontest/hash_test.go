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

// fnv1aVectors are the sums of FNV-1a of 64 bits for the keys of the golden table of
// the tests of Go's hash/fnv.
var fnv1aVectors = []partitiontest.Vector{
	{Key: []byte(""), Sum: 0xcbf29ce484222325},
	{Key: []byte("a"), Sum: 0xaf63dc4c8601ec8c},
	{Key: []byte("ab"), Sum: 0x089c4407b545986a},
	{Key: []byte("abc"), Sum: 0xe71fa2190541574b},
}

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

// TestAssertHash runs the laws of a hash against fnv1a, which keeps every law.
func TestAssertHash(t *testing.T) {
	t.Parallel()
	partitiontest.AssertHash(t, fnv1a{}, fnv1aVersion, fnv1aVectors...)
}
