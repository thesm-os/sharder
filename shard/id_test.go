// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard_test

import (
	"encoding/hex"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/kanon/kanontest"

	"go.thesmos.sh/sharder/shard"
)

// recordedIDHex pins the encoding of recordedID, which every message and record of
// a shard persists: the 16 bytes of its identifier, without a length.
const recordedIDHex = "1112131415161718191a1b1c1d1e1f20"

// recordedID is the shard ID whose encoding recordedIDHex pins.
var recordedID = shard.ID{ID: id.New128([16]byte{
	0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
	0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
})}

// TestID checks the encoding of a shard ID, which the codecs of the package write
// through the binary methods of the identifier that ID embeds.
func TestID(t *testing.T) {
	t.Parallel()

	t.Run("AppendBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("appends the 16 bytes of the identifier", func(t *testing.T) {
			t.Parallel()
			got, err := recordedID.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode an ID")
			assert.Equal(
				t,
				hex.EncodeToString(got),
				recordedIDHex,
				"AppendBinary must write the bytes of the identifier",
			)
		})

		t.Run("appends no bytes for the zero ID", func(t *testing.T) {
			t.Parallel()
			got, err := shard.ID{}.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode the zero ID")
			assert.Empty(t, got, "AppendBinary must write no bytes for the zero ID")
		})
	})

	t.Run("UnmarshalBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the ID of the recorded encoding", func(t *testing.T) {
			t.Parallel()
			var got shard.ID
			assert.NoError(
				t,
				got.UnmarshalBinary(decodeHex(t, recordedIDHex)),
				"UnmarshalBinary must decode the recorded encoding",
			)
			assert.Equal(t, got, recordedID, "UnmarshalBinary must return the recorded ID")
		})

		t.Run("returns the zero ID for no bytes", func(t *testing.T) {
			t.Parallel()
			got := recordedID
			assert.NoError(t, got.UnmarshalBinary(nil), "UnmarshalBinary must decode no bytes")
			assert.Equal(t, got, shard.ID{}, "UnmarshalBinary must return the zero ID for no bytes")
		})
	})

	t.Run("ExactKanon", func(t *testing.T) {
		t.Parallel()

		t.Run("keeps the guarantees of an exact kanon type", func(t *testing.T) {
			t.Parallel()
			kanontest.RunExact[shard.ID](t)
		})
	})
}

// TestIDAllocs checks the allocation contract of ID. MaxAllocs counts the
// allocations of the whole process, so the test does not run in parallel.
func TestIDAllocs(t *testing.T) {
	raw := [16]byte{0x11}
	other := shard.ID{ID: id.New128(raw)}
	placements := map[shard.ID]int{recordedID: 1}
	buf := make([]byte, 0, id.Size128)

	t.Run("constructs an ID without an allocation", func(t *testing.T) {
		var got shard.ID
		expect.MaxAllocs(t, func() { got = shard.ID{ID: id.New128(raw)} }, 0, "constructing an ID must not allocate")
		assert.Equal(t, got, other, "the test must measure the construction of the ID")
	})

	t.Run("compares two IDs without an allocation", func(t *testing.T) {
		var got bool
		expect.MaxAllocs(t, func() { got = recordedID == other }, 0, "comparing two IDs must not allocate")
		assert.False(t, got, "the test must compare two IDs that differ")
	})

	t.Run("looks up a map keyed by ID without an allocation", func(t *testing.T) {
		var got int
		expect.MaxAllocs(
			t,
			func() { got = placements[recordedID] },
			0,
			"a lookup in a map keyed by ID must not allocate",
		)
		assert.Equal(t, got, 1, "the test must look up a key of the map")
	})

	t.Run("AppendBinary", func(t *testing.T) {
		var got []byte
		expect.MaxAllocs(t, func() { got, _ = recordedID.AppendBinary(buf[:0]) }, 0,
			"AppendBinary into a buffer with room must not allocate")
		assert.Length(t, got, id.Size128, "the test must measure the encoding of an ID")
	})
}

// decodeHex returns the bytes of the hexadecimal text s, and fails tb when s is not
// hexadecimal.
func decodeHex(tb testing.TB, s string) []byte {
	tb.Helper()

	data, err := hex.DecodeString(s)
	assert.NoError(tb, err, "the recorded encoding must be hexadecimal")

	return data
}
