// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard_test

import (
	"encoding/hex"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/shard"
)

// recordedMappingHex pins the encoding of recordedMapping, which the root scope of
// every namespace persists: field 1 is the partitioner fixed-slots, field 2 the
// configuration {"slots":1024}, field 3 the hash xxhash64, and field 4 its version 1.
const recordedMappingHex = "0a0b" + "66697865642d736c6f7473" +
	"120e" + "7b22736c6f7473223a313032347d" +
	"1a08" + "7878686173683634" +
	"2001"

// recordedMapping is the mapping whose encoding recordedMappingHex pins.
var recordedMapping = shard.Mapping{
	Partitioner: "fixed-slots",
	Config:      []byte(`{"slots":1024}`),
	Hash:        "xxhash64",
	HashVersion: 1,
}

// TestMapping checks the recorded encoding of a Mapping.
func TestMapping(t *testing.T) {
	t.Parallel()

	t.Run("AppendBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("appends the recorded encoding of a Mapping", func(t *testing.T) {
			t.Parallel()
			m := recordedMapping
			got, err := m.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode the Mapping")
			assert.Equal(
				t,
				hex.EncodeToString(got),
				recordedMappingHex,
				"AppendBinary must write fields 1 to 4 as recorded",
			)
		})
	})

	t.Run("UnmarshalBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the Mapping of the recorded encoding", func(t *testing.T) {
			t.Parallel()
			var got shard.Mapping
			assert.NoError(
				t,
				got.UnmarshalBinary(decodeHex(t, recordedMappingHex)),
				"UnmarshalBinary must decode the recorded encoding",
			)
			assert.Equal(t, got, recordedMapping, "UnmarshalBinary must return the recorded Mapping")
		})
	})
}

// TestMappingAllocs checks the allocation contract of Mapping. MaxAllocs counts the
// allocations of the whole process, so the test does not run in parallel.
func TestMappingAllocs(t *testing.T) {
	partitioner, config, hash, version := recordedMapping.Partitioner, recordedMapping.Config, recordedMapping.Hash,
		recordedMapping.HashVersion

	t.Run("constructs a Mapping without an allocation", func(t *testing.T) {
		var got shard.Mapping
		expect.MaxAllocs(t, func() {
			got = shard.Mapping{Partitioner: partitioner, Config: config, Hash: hash, HashVersion: version}
		}, 0, "constructing a Mapping must not allocate")
		assert.Equal(t, got, recordedMapping, "the test must measure the construction of the Mapping")
	})
}
