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

// recordedIncarnationHex pins the encoding of recordedIncarnation, which every
// scope, fence token, operation and message of a namespace persists: the 16 bytes of
// its identifier, without a length.
const recordedIncarnationHex = "0102030405060708090a0b0c0d0e0f10"

// recordedIncarnation is the namespace incarnation whose encoding
// recordedIncarnationHex pins.
var recordedIncarnation = shard.NamespaceIncarnation{ID: id.New128([16]byte{
	0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
	0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
})}

// TestNamespaceIncarnation checks the encoding of a namespace incarnation, which the
// codecs of the package write through the binary methods of the identifier that
// NamespaceIncarnation embeds.
func TestNamespaceIncarnation(t *testing.T) {
	t.Parallel()

	t.Run("AppendBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("appends the 16 bytes of the identifier", func(t *testing.T) {
			t.Parallel()
			got, err := recordedIncarnation.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode a namespace incarnation")
			assert.Equal(t, hex.EncodeToString(got), recordedIncarnationHex,
				"AppendBinary must write the bytes of the identifier")
		})

		t.Run("appends no bytes for the zero incarnation", func(t *testing.T) {
			t.Parallel()
			got, err := shard.NamespaceIncarnation{}.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode the zero incarnation")
			assert.Empty(t, got, "AppendBinary must write no bytes for the zero incarnation")
		})
	})

	t.Run("UnmarshalBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the incarnation of the recorded encoding", func(t *testing.T) {
			t.Parallel()
			var got shard.NamespaceIncarnation
			assert.NoError(t, got.UnmarshalBinary(decodeHex(t, recordedIncarnationHex)),
				"UnmarshalBinary must decode the recorded encoding")
			assert.Equal(t, got, recordedIncarnation, "UnmarshalBinary must return the recorded incarnation")
		})

		t.Run("returns the zero incarnation for no bytes", func(t *testing.T) {
			t.Parallel()
			got := recordedIncarnation
			assert.NoError(t, got.UnmarshalBinary(nil), "UnmarshalBinary must decode no bytes")
			assert.Equal(
				t,
				got,
				shard.NamespaceIncarnation{},
				"UnmarshalBinary must return the zero incarnation for no bytes",
			)
		})
	})

	t.Run("ExactKanon", func(t *testing.T) {
		t.Parallel()

		t.Run("keeps the guarantees of an exact kanon type", func(t *testing.T) {
			t.Parallel()
			kanontest.RunExact[shard.NamespaceIncarnation](t)
		})
	})
}

// TestNamespaceIncarnationAllocs checks the allocation contract of
// NamespaceIncarnation. MaxAllocs counts the allocations of the whole process, so the
// test does not run in parallel.
func TestNamespaceIncarnationAllocs(t *testing.T) {
	raw := [16]byte{0x01}
	other := shard.NamespaceIncarnation{ID: id.New128(raw)}
	buf := make([]byte, 0, id.Size128)

	t.Run("constructs an incarnation without an allocation", func(t *testing.T) {
		var got shard.NamespaceIncarnation
		expect.MaxAllocs(t, func() { got = shard.NamespaceIncarnation{ID: id.New128(raw)} }, 0,
			"constructing an incarnation must not allocate")
		assert.Equal(t, got, other, "the test must measure the construction of the incarnation")
	})

	t.Run("compares two incarnations without an allocation", func(t *testing.T) {
		var got bool
		expect.MaxAllocs(
			t,
			func() { got = recordedIncarnation == other },
			0,
			"comparing two incarnations must not allocate",
		)
		assert.False(t, got, "the test must compare two incarnations that differ")
	})

	t.Run("AppendBinary", func(t *testing.T) {
		var got []byte
		expect.MaxAllocs(t, func() { got, _ = recordedIncarnation.AppendBinary(buf[:0]) }, 0,
			"AppendBinary into a buffer with room must not allocate")
		assert.Length(t, got, id.Size128, "the test must measure the encoding of an incarnation")
	})
}
