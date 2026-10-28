// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package fixedslots_test

import (
	"encoding/hex"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/partition/partitiontest"
	"go.thesmos.sh/sharder/partition/xxhash64"
	"go.thesmos.sh/sharder/shard"
)

const (
	// recordedName is the registered name of the partitioner, which the mappings of
	// namespaces record.
	recordedName = "fixed-slots"
	// recordedConfig is the configuration tagged as the mappings of namespaces record
	// it.
	recordedConfig = `{"hash":"xxhash64","slots":1024,"hashTags":true}`
	// recordedHashVersion is the version of xxhash64, which the mappings of
	// namespaces record.
	recordedHashVersion = 1
	// versionedVersion is the version that the double of the hash returns in the test
	// of the hash version of the mapping.
	versionedVersion = 7
	// plainKey is a key without a hash tag.
	plainKey = "user:42"
	// plainPosition is the position of plainKey under plain: slot 450.
	plainPosition = "000001c2"
	// taggedKey is a key with the hash tag 42.
	taggedKey = "user:{42}:cart"
	// taggedPosition is the position of taggedKey under tagged: slot 697, the slot of
	// the key 42.
	taggedPosition = "000002b9"
)

// The tests use these configurations.
var (
	// plain is 1024 slots of xxhash64 without hash tags.
	plain = fixedslots.Config{Hash: xxhash64.Name, Slots: 1024}
	// tagged is 1024 slots of xxhash64 with hash tags.
	tagged = fixedslots.Config{Hash: xxhash64.Name, Slots: 1024, HashTags: true}
	// plain1000 is 1000 slots of xxhash64 without hash tags.
	plain1000 = fixedslots.Config{Hash: xxhash64.Name, Slots: 1000}
	// tagged1000 is 1000 slots of xxhash64 with hash tags.
	tagged1000 = fixedslots.Config{Hash: xxhash64.Name, Slots: 1000, HashTags: true}
	// four is 4 slots of xxhash64, whose spans the tests of Initial list.
	four = fixedslots.Config{Hash: xxhash64.Name, Slots: 4}
)

// slot0 to slot4 are the positions of the slots 0 to 4. The partitioner of four has the
// slots 0 to 3, so slot4 is not the position of one of its slots.
var (
	slot0 = []byte{0, 0, 0, 0}
	slot1 = []byte{0, 0, 0, 1}
	slot2 = []byte{0, 0, 0, 2}
	slot3 = []byte{0, 0, 0, 3}
	slot4 = []byte{0, 0, 0, 4}
)

// TestPartitioner checks the construction, the positions, the mapping and the spans
// of fixed-slots.
func TestPartitioner(t *testing.T) {
	t.Parallel()

	t.Run("New", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			cfg  fixedslots.Config
			hash partition.Hash
		}{
			{name: "returns ErrConfig for a Config that is not valid", cfg: fixedslots.Config{}, hash: xxhash64.Hash{}},
			{name: "returns ErrConfig for a nil hash", cfg: plain},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				p, err := fixedslots.New(tt.cfg, tt.hash)
				expect.ErrorIs(t, err, fixedslots.ErrConfig, "New must return ErrConfig")
				expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of New must classify as Invalid")
				expect.Nil(t, p, "New must not return a partitioner with an error")
			})
		}
	})

	t.Run("Position", func(t *testing.T) {
		t.Parallel()

		// Each position is the slot of XXH64 with seed 0 of the hashed bytes, modulo the
		// slots. The reference implementation of xxHash v0.8.3 computed each sum.
		tests := []struct {
			name string
			cfg  fixedslots.Config
			key  string
			want string
		}{
			{name: "returns slot 409 for the empty key", cfg: plain, key: "", want: "00000199"},
			{name: "returns slot 450 for a key", cfg: plain, key: plainKey, want: plainPosition},
			{name: "returns slot 386 of 1000 slots for a key", cfg: plain1000, key: plainKey, want: "00000182"},
			{name: "returns the slot of the whole key without hash tags", cfg: plain, key: taggedKey, want: "0000005d"},
			{
				name: "returns the slot of the hash tag with hash tags",
				cfg:  tagged, key: taggedKey, want: taggedPosition,
			},
			{
				name: "returns the slot of the whole key when it has no {",
				cfg:  tagged, key: plainKey, want: plainPosition,
			},
			{
				name: "returns the slot of the same hash tag for another key",
				cfg:  tagged, key: "user:{42}:orders", want: taggedPosition,
			},
			{
				name: "returns slot 401 of 1000 slots for the hash tag",
				cfg:  tagged1000, key: taggedKey, want: "00000191",
			},
			{
				name: "returns the slot of a hash tag at the start of the key",
				cfg:  tagged, key: "{user1000}.following", want: "00000317",
			},
			{
				name: "returns the slot of the whole key when the first hash tag is empty",
				cfg:  tagged, key: "foo{}{bar}", want: "000001e2",
			},
			{
				name: "returns the slot of the bytes up to the first } after the first {",
				cfg:  tagged, key: "foo{{bar}}zap", want: "00000368",
			},
			{
				name: "returns the slot of the first hash tag of two",
				cfg:  tagged, key: "foo{bar}{zap}", want: "00000259",
			},
			{
				name: "returns the slot of the whole key when it ends with a {",
				cfg:  tagged, key: "a{", want: "00000329",
			},
			{
				name: "returns the slot of the whole key when no } follows the {",
				cfg:  tagged, key: "user:{42", want: "000000af",
			},
			{
				name: "returns the slot of the whole key when the } precedes the {",
				cfg:  tagged, key: "}{", want: "00000244",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				p := newPartitioner(t, tt.cfg)
				assert.Equal(t, hex.EncodeToString(p.Position(nil, []byte(tt.key))), tt.want,
					"Position must return the slot of XXH64 of the hashed bytes")
			})
		}
	})

	t.Run("Mapping", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the recorded mapping", func(t *testing.T) {
			t.Parallel()
			assert.Equal(
				t,
				newPartitioner(t, tagged).Mapping(),
				shard.Mapping{
					Partitioner: recordedName,
					Hash:        xxhash64.Name,
					Config:      []byte(recordedConfig),
					HashVersion: recordedHashVersion,
				},
				"Mapping must return the mapping that namespaces record",
			)
		})

		t.Run("returns the version of the hash", func(t *testing.T) {
			t.Parallel()
			versioned := partitiontest.NewHashStub(t, partitiontest.HashStubDelegateTo(xxhash64.Hash{}),
				partitiontest.WithHashVersion(func() uint32 { return versionedVersion }))
			p, err := fixedslots.New(tagged, versioned)
			assert.NoError(t, err, "New must build the partitioner")
			assert.Equal(t, p.Mapping().HashVersion, versionedVersion, "Mapping must return the version of the hash")
		})
	})

	t.Run("Initial", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			give shard.Range
			want []partition.Span
		}{
			{
				name: "returns one span per slot of the position space",
				give: shard.Range{},
				want: []partition.Span{
					{Range: shard.Range{End: slot1}},
					{Range: shard.Range{Start: slot1, End: slot2}},
					{Range: shard.Range{Start: slot2, End: slot3}},
					{Range: shard.Range{Start: slot3}},
				},
			},
			{
				name: "returns the spans of the slots between two bounds",
				give: shard.Range{Start: slot1, End: slot3},
				want: []partition.Span{
					{Range: shard.Range{Start: slot1, End: slot2}},
					{Range: shard.Range{Start: slot2, End: slot3}},
				},
			},
			{
				name: "returns one span for a range of one slot",
				give: shard.Range{Start: slot2, End: slot3},
				want: []partition.Span{{Range: shard.Range{Start: slot2, End: slot3}}},
			},
			{
				name: "returns the spans from the first position to a bound",
				give: shard.Range{End: slot2},
				want: []partition.Span{
					{Range: shard.Range{End: slot1}},
					{Range: shard.Range{Start: slot1, End: slot2}},
				},
			},
			{
				name: "returns the spans from a bound to the end of the position space",
				give: shard.Range{Start: slot2},
				want: []partition.Span{
					{Range: shard.Range{Start: slot2, End: slot3}},
					{Range: shard.Range{Start: slot3}},
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				spans, err := newPartitioner(t, four).Initial(tt.give)
				assert.NoError(t, err, "Initial must divide the range")
				assert.Equal(t, spans, tt.want, "Initial must return one span per slot of the range")
			})
		}

		refused := []struct {
			name string
			give shard.Range
		}{
			{name: "returns ErrRange for a start of 3 bytes", give: shard.Range{Start: slot1[1:]}},
			{name: "returns ErrRange for an end of 5 bytes", give: shard.Range{End: append([]byte{0}, slot1...)}},
			{name: "returns ErrRange for a start at the position of slot 0", give: shard.Range{Start: slot0}},
			{name: "returns ErrRange for an end above the last slot", give: shard.Range{End: slot4}},
			{name: "returns ErrRange for a start equal to the end", give: shard.Range{Start: slot2, End: slot2}},
			{name: "returns ErrRange for a start above the end", give: shard.Range{Start: slot3, End: slot1}},
		}
		for _, tt := range refused {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				spans, err := newPartitioner(t, four).Initial(tt.give)
				expect.ErrorIs(t, err, partition.ErrRange, "Initial must return ErrRange")
				expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of Initial must classify as Invalid")
				expect.Nil(t, spans, "Initial must not return spans with an error")
			})
		}

		t.Run("returns one error per bound that is not the position of a slot", func(t *testing.T) {
			t.Parallel()
			_, err := newPartitioner(t, four).Initial(shard.Range{Start: slot0, End: slot4})
			joined := assert.ErrorAs[interface{ Unwrap() []error }](t, err,
				"Initial must join the problems of the bounds")
			assert.Length(t, joined.Unwrap(), 2, "Initial must return a problem for each of the two bounds")
		})
	})
}

// newTagged returns fixed-slots with the configuration tagged, the subject of the suite
// of partition.Partitioner.
func newTagged(h *partitiontest.PartitionerHandle) *fixedslots.Partitioner {
	return newPartitioner(h, tagged)
}

// newPartitioner returns fixed-slots with the configuration cfg and the hash
// xxhash64. It stops tb when New fails.
func newPartitioner(tb assert.TB, cfg fixedslots.Config) *fixedslots.Partitioner {
	tb.Helper()
	p, err := fixedslots.New(cfg, xxhash64.Hash{})
	assert.NoError(tb, err, "New must build the partitioner")
	return p
}
