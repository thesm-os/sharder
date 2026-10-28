// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package xxhash64_test

import (
	"testing"

	"go.dokimi.dev/assert"

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

// TestHash checks the registered name, the sums and the version of the hash.
func TestHash(t *testing.T) {
	t.Parallel()

	t.Run("Name", func(t *testing.T) {
		t.Parallel()

		t.Run("is the name that mappings record", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, xxhash64.Name, recordedName, "Name must be the name that the mappings of namespaces record")
		})
	})

	t.Run("Sum64", func(t *testing.T) {
		t.Parallel()

		// The rows of seed 0 of the table XSUM_XXH64_testdata in the sanity check of
		// xxHash v0.8.3. The key of each row is a prefix of the buffer of the check.
		tests := []struct {
			name string
			give []byte
			want uint64
		}{
			{name: "returns the XXH64 of the empty key", give: sanity[:0], want: 0xEF46DB3751D8E999},
			{name: "returns the XXH64 of a key of 1 byte", give: sanity[:1], want: 0xE934A84ADB052768},
			{name: "returns the XXH64 of a key of 4 bytes", give: sanity[:4], want: 0x9136A0DCA57457EE},
			{name: "returns the XXH64 of a key of 14 bytes", give: sanity[:14], want: 0x8282DCC4994E35C8},
			{name: "returns the XXH64 of a key of 222 bytes", give: sanity[:222], want: 0xB641AE8CB691C174},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, xxhash64.Hash{}.Sum64(tt.give), tt.want,
					"Sum64 must return the sum of the sanity check of xxHash")
			})
		}
	})

	t.Run("Version", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the version that mappings record", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, xxhash64.Hash{}.Version(), recordedVersion,
				"Version must return the version that the mappings of namespaces record")
		})
	})
}

// newHash returns the zero Hash, the subject of the suite of partition.Hash.
func newHash() xxhash64.Hash {
	return xxhash64.Hash{}
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
