// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package xxhash64_test

import (
	"strconv"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/partition/partitiontest"
	"go.thesmos.sh/sharder/partition/xxhash64"
)

const (
	// recordedName is the registered name of the hash, which the mappings of
	// namespaces record.
	recordedName = "xxhash64"
	// recordedVersion is the version of the hash, which the mappings of namespaces
	// record.
	recordedVersion = 1
)

// The generator of the sanity check of xxHash fills the buffer whose prefixes are the
// keys of the XXH64 vectors.
const (
	// sanitySeed is the first state of the generator, PRIME32 of xxHash.
	sanitySeed = 2654435761
	// sanityMultiplier multiplies the state of the generator after each byte, PRIME64
	// of xxHash.
	sanityMultiplier = 11400714785074694797
	// sanityLength is the length of the longest key of the vectors.
	sanityLength = 222
)

// sanity is the buffer of the sanity check, whose prefixes are the keys of vectors.
var sanity = sanityBuffer(sanityLength)

// vectors are the rows of seed 0 of the table XSUM_XXH64_testdata in the sanity
// check of xxHash v0.8.3.
var vectors = []partitiontest.Vector{
	{Key: sanity[:0], Sum: 0xEF46DB3751D8E999},
	{Key: sanity[:1], Sum: 0xE934A84ADB052768},
	{Key: sanity[:4], Sum: 0x9136A0DCA57457EE},
	{Key: sanity[:14], Sum: 0x8282DCC4994E35C8},
	{Key: sanity[:222], Sum: 0xB641AE8CB691C174},
}

// TestHash checks the registered name of the hash.
func TestHash(t *testing.T) {
	t.Parallel()

	t.Run("Name", func(t *testing.T) {
		t.Parallel()

		t.Run("is the name that mappings record", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, xxhash64.Name, recordedName, "Name must be the name that the mappings of namespaces record")
		})
	})
}

// TestHashConformance runs the laws of a hash against Hash, with the XXH64 vectors of
// xxHash.
func TestHashConformance(t *testing.T) {
	t.Parallel()
	partitiontest.AssertHash(t, xxhash64.Hash{}, recordedVersion, vectors...)
}

// TestHashAllocs checks the allocation contract of Hash through partition.Hash, as a
// partitioner calls it. MaxAllocs counts the allocations of the whole process, so the
// test does not run in parallel.
func TestHashAllocs(t *testing.T) {
	var h partition.Hash = xxhash64.Hash{}

	t.Run("Sum64", func(t *testing.T) {
		v := vectors[len(vectors)-1]
		var got uint64
		expect.MaxAllocs(t, func() { got = h.Sum64(v.Key) }, 0, "Sum64 must not allocate")
		assert.Equal(t, got, v.Sum, "the test must measure the sum of the vector")
	})
}

// BenchmarkHash reports the cost of Sum64 for the key of each vector, and fails when
// Sum64 allocates.
func BenchmarkHash(b *testing.B) {
	var h partition.Hash = xxhash64.Hash{}

	b.Run("Sum64", func(b *testing.B) {
		for _, v := range vectors {
			b.Run(strconv.Itoa(len(v.Key)), func(b *testing.B) {
				var got uint64

				c := bench.Start(b).MaxAllocs(0)
				defer c.End()

				for c.Loop() {
					got = h.Sum64(v.Key)
				}

				assert.Equal(b, got, v.Sum, "the benchmark must measure the sum of the vector")
			})
		}
	})
}

// sanityBuffer returns the first n bytes that the generator of the sanity check of
// xxHash produces. Each byte is the top byte of the state of the generator, and the
// state then multiplies by sanityMultiplier.
func sanityBuffer(n int) []byte {
	buf := make([]byte, n)
	state := uint64(sanitySeed)
	for i := range buf {
		buf[i] = byte(state >> 56)
		state *= sanityMultiplier
	}
	return buf
}
