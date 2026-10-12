// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard_test

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/kanon/kanontest"

	"go.thesmos.sh/sharder/shard"
)

// recordedIDHex pins the encoding of recordedID. Every message and record of a shard
// persists the 16 bytes of the identifier without a length.
const recordedIDHex = "1112131415161718191a1b1c1d1e1f20"

// The texts of the tests. The body of a proposal contains the text of a shard ID.
const (
	// recordedIDText pins the text of recordedID: the canonical form of its ULID.
	recordedIDText = "0H289H858P2WC1J6GV3GEHW7S0"
	// zeroULIDText is the canonical form of the ULID of 16 zero bytes.
	zeroULIDText = "00000000000000000000000000"
	// malformedText is text that is not the canonical form of a ULID.
	malformedText = "not-a-ulid"
)

// recordedID is the shard ID whose encoding recordedIDHex pins.
var recordedID = shard.ID{ID128: id.ID128{
	0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
	0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
}}

// TestID checks the zero shard ID, the order of shard IDs and their encoding. A shard
// ID encodes as the bytes that core's id.ID writes for the same identifier.
func TestID(t *testing.T) {
	t.Parallel()

	t.Run("IsZero", func(t *testing.T) {
		t.Parallel()

		t.Run("reports true for the zero ID", func(t *testing.T) {
			t.Parallel()
			assert.True(t, shard.ID{}.IsZero(), "the zero ID must be no shard")
		})

		t.Run("reports false for an ID with one byte that is not zero", func(t *testing.T) {
			t.Parallel()
			assert.False(t, shard.ID{ID128: id.ID128{15: 1}}.IsZero(), "an ID with a byte above zero must be a shard")
		})
	})

	t.Run("Compare", func(t *testing.T) {
		t.Parallel()

		lower := shard.ID{ID128: id.ID128{0x11}}
		higher := shard.ID{ID128: id.ID128{0x12}}
		tests := []struct {
			name string
			a, b shard.ID
			want int
		}{
			{name: "returns -1 for an ID whose first byte is lower", a: lower, b: higher, want: -1},
			{name: "returns 0 for the same ID", a: recordedID, b: recordedID, want: 0},
			{name: "returns +1 for an ID whose first byte is higher", a: higher, b: lower, want: 1},
			{name: "returns -1 for the zero ID against another ID", a: shard.ID{}, b: lower, want: -1},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				expect.Equal(t, tt.a.Compare(tt.b), tt.want, "Compare must order IDs by their bytes")
				expect.Equal(t, tt.a.Compare(tt.b), tt.a.ID().Compare(tt.b.ID()),
					"Compare must return what core's id.ID returns for the identifiers")
			})
		}
	})

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

		t.Run("appends the bytes that the id.ID of the identifier appends", func(t *testing.T) {
			t.Parallel()
			got, err := recordedID.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode an ID")
			want, err := recordedID.ID().AppendBinary(nil)
			assert.NoError(t, err, "the id.ID of the identifier must encode")
			assert.Equal(t, got, want, "AppendBinary must write the bytes that core's id.ID writes")
		})

		t.Run("appends no bytes for the zero ID", func(t *testing.T) {
			t.Parallel()
			got, err := shard.ID{}.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode the zero ID")
			assert.Empty(t, got, "AppendBinary must write no bytes for the zero ID")
		})
	})

	t.Run("MarshalBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the bytes that AppendBinary appends", func(t *testing.T) {
			t.Parallel()
			got, err := recordedID.MarshalBinary()
			assert.NoError(t, err, "MarshalBinary must encode an ID")
			assert.Equal(
				t,
				hex.EncodeToString(got),
				recordedIDHex,
				"MarshalBinary must write the bytes of the identifier",
			)
		})

		t.Run("returns no bytes for the zero ID", func(t *testing.T) {
			t.Parallel()
			got, err := shard.ID{}.MarshalBinary()
			assert.NoError(t, err, "MarshalBinary must encode the zero ID")
			assert.Empty(t, got, "MarshalBinary must write no bytes for the zero ID")
		})
	})

	t.Run("SizeKanon", func(t *testing.T) {
		t.Parallel()

		t.Run("returns 16 for an ID", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, recordedID.SizeKanon(), id.Size128, "an ID must encode as 16 bytes")
		})

		t.Run("returns 0 for the zero ID", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, shard.ID{}.SizeKanon(), 0, "the zero ID must encode as no bytes")
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

		tests := []struct {
			name string
			give []byte
			want error
		}{
			{name: "returns id.ErrSize for 15 bytes", give: make([]byte, 15), want: id.ErrSize},
			{
				name: "returns ErrID for the 20 bytes of a wider identifier",
				give: make([]byte, id.Size160),
				want: shard.ErrID,
			},
			{
				name: "returns ErrID for the 32 bytes of a wider identifier",
				give: make([]byte, id.Size256),
				want: shard.ErrID,
			},
			{name: "returns ErrID for 16 zero bytes", give: make([]byte, id.Size128), want: shard.ErrID},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				got := recordedID
				err := got.UnmarshalBinary(tt.give)
				expect.ErrorIs(t, err, tt.want, "UnmarshalBinary must refuse bytes that are not the encoding of an ID")
				expect.ErrorIs(t, err, shard.ErrID, "every refusal of UnmarshalBinary must wrap ErrID")
				expect.Equal(
					t,
					errs.Classify(err),
					errs.Invalid,
					"the error of UnmarshalBinary must classify as Invalid",
				)
				expect.Equal(t, got, recordedID, "UnmarshalBinary must leave the ID unchanged")
			})
		}
	})

	t.Run("ExactKanon", func(t *testing.T) {
		t.Parallel()

		t.Run("keeps the guarantees of an exact kanon type", func(t *testing.T) {
			t.Parallel()
			kanontest.RunExact[shard.ID](t)
		})
	})

	t.Run("MarshalText", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the canonical form of the ULID", func(t *testing.T) {
			t.Parallel()
			got, err := recordedID.MarshalText()
			assert.NoError(t, err, "MarshalText must write an ID")
			expect.Equal(t, string(got), recordedIDText, "MarshalText must write the recorded text of the ID")
			expect.Equal(t, string(got), ulid.Format(recordedID.ID()), "MarshalText must write what ulid.Format writes")
		})

		t.Run("returns no text for the zero ID", func(t *testing.T) {
			t.Parallel()
			got, err := shard.ID{}.MarshalText()
			assert.NoError(t, err, "MarshalText must write the zero ID")
			assert.Empty(t, got, "MarshalText must write no text for the zero ID")
		})

		t.Run("writes the text as a JSON string", func(t *testing.T) {
			t.Parallel()
			got, err := json.Marshal(recordedID)
			assert.NoError(t, err, "encoding/json must encode an ID")
			assert.Equal(t, string(got), `"`+recordedIDText+`"`, "encoding/json must write the text of the ID")
		})
	})

	t.Run("UnmarshalText", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the ID of the canonical form of its ULID", func(t *testing.T) {
			t.Parallel()
			var got shard.ID
			assert.NoError(t, got.UnmarshalText([]byte(recordedIDText)), "UnmarshalText must decode the recorded text")
			assert.Equal(t, got, recordedID, "UnmarshalText must return the recorded ID")
		})

		t.Run("returns the zero ID for no text", func(t *testing.T) {
			t.Parallel()
			got := recordedID
			assert.NoError(t, got.UnmarshalText(nil), "UnmarshalText must decode no text")
			assert.Equal(t, got, shard.ID{}, "UnmarshalText must return the zero ID for no text")
		})

		t.Run("undoes MarshalText", func(t *testing.T) {
			t.Parallel()
			assert.RoundTrip(t, shard.ID.MarshalText, func(text []byte) (shard.ID, error) {
				var got shard.ID
				err := got.UnmarshalText(text)
				return got, err
			}, recordedID, "UnmarshalText must return the ID whose text MarshalText wrote")
		})

		tests := []struct {
			name string
			give string
		}{
			{name: "returns ErrID for text that is not a ULID", give: malformedText},
			{name: "returns ErrID for the text of the zero ULID", give: zeroULIDText},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				got := recordedID
				err := got.UnmarshalText([]byte(tt.give))
				expect.ErrorIs(t, err, shard.ErrID, "UnmarshalText must refuse text that is not the text of an ID")
				expect.Equal(t, got, recordedID, "UnmarshalText must leave the ID unchanged")
			})
		}
	})
}

// TestIDAllocs checks the allocation contract of ID. MaxAllocs counts the
// allocations of the whole process, so the test does not run in parallel.
func TestIDAllocs(t *testing.T) {
	raw := id.ID128{0x11}
	other := shard.ID{ID128: raw}
	placements := map[shard.ID]int{recordedID: 1}
	buf := make([]byte, 0, id.Size128)
	encoded := decodeHex(t, recordedIDHex)

	t.Run("constructs an ID without an allocation", func(t *testing.T) {
		var got shard.ID
		expect.MaxAllocs(t, func() { got = shard.ID{ID128: raw} }, 0, "constructing an ID must not allocate")
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

	t.Run("IsZero", func(t *testing.T) {
		var got bool
		expect.MaxAllocs(t, func() { got = recordedID.IsZero() }, 0, "IsZero must not allocate")
		assert.False(t, got, "the test must measure an ID that is not zero")
	})

	t.Run("Compare", func(t *testing.T) {
		var got int
		expect.MaxAllocs(t, func() { got = recordedID.Compare(other) }, 0, "Compare must not allocate")
		assert.Equal(t, got, 1, "the test must compare two IDs that differ")
	})

	t.Run("AppendBinary", func(t *testing.T) {
		var got []byte
		expect.MaxAllocs(t, func() { got, _ = recordedID.AppendBinary(buf[:0]) }, 0,
			"AppendBinary into a buffer with room must not allocate")
		assert.Length(t, got, id.Size128, "the test must measure the encoding of an ID")
	})

	t.Run("MarshalBinary", func(t *testing.T) {
		t.Run("allocates the returned slice", func(t *testing.T) {
			var got []byte
			expect.MaxAllocs(t, func() { got, _ = recordedID.MarshalBinary() }, 1,
				"MarshalBinary must allocate only the returned slice")
			assert.Length(t, got, id.Size128, "the test must measure the encoding of an ID")
		})

		t.Run("does not allocate for the zero ID", func(t *testing.T) {
			var got []byte
			expect.MaxAllocs(t, func() { got, _ = shard.ID{}.MarshalBinary() }, 0,
				"MarshalBinary of the zero ID must not allocate")
			assert.Empty(t, got, "the test must measure the encoding of the zero ID")
		})
	})

	t.Run("SizeKanon", func(t *testing.T) {
		var got int
		expect.MaxAllocs(t, func() { got = recordedID.SizeKanon() }, 0, "SizeKanon must not allocate")
		assert.Equal(t, got, id.Size128, "the test must measure the size of an ID")
	})

	t.Run("AppendKanon", func(t *testing.T) {
		var got []byte
		expect.MaxAllocs(t, func() { got = recordedID.AppendKanon(buf[:0]) }, 0,
			"AppendKanon into a buffer with room must not allocate")
		assert.Length(t, got, id.Size128, "the test must measure the encoding of an ID")
	})

	t.Run("UnmarshalBinary", func(t *testing.T) {
		var got shard.ID
		var err error
		expect.MaxAllocs(t, func() { err = got.UnmarshalBinary(encoded) }, 0,
			"UnmarshalBinary of a valid encoding must not allocate")
		assert.NoError(t, err, "the test must measure a valid encoding")
		assert.Equal(t, got, recordedID, "the test must measure the decode of the recorded ID")
	})
}

// BenchmarkID reports the cost of the methods of ID that the planner and the codecs
// call, and fails when one allocates.
func BenchmarkID(b *testing.B) {
	other := shard.ID{ID128: id.ID128{0x11}}
	buf := make([]byte, 0, id.Size128)
	encoded := decodeHex(b, recordedIDHex)

	b.Run("IsZero", func(b *testing.B) {
		var got bool

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			got = recordedID.IsZero()
		}

		assert.False(b, got, "the benchmark must measure an ID that is not zero")
	})

	b.Run("Compare", func(b *testing.B) {
		var got int

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			got = recordedID.Compare(other)
		}

		assert.Equal(b, got, 1, "the benchmark must compare two IDs that differ")
	})

	b.Run("AppendBinary", func(b *testing.B) {
		var got []byte
		var err error

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			got, err = recordedID.AppendBinary(buf[:0])
		}

		assert.NoError(b, err, "the benchmark must measure an encoding that succeeds")
		assert.Length(b, got, id.Size128, "the benchmark must measure the encoding of an ID")
	})

	b.Run("SizeKanon", func(b *testing.B) {
		var got int

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			got = recordedID.SizeKanon()
		}

		assert.Equal(b, got, id.Size128, "the benchmark must measure the size of an ID")
	})

	b.Run("AppendKanon", func(b *testing.B) {
		var got []byte

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			got = recordedID.AppendKanon(buf[:0])
		}

		assert.Length(b, got, id.Size128, "the benchmark must measure the encoding of an ID")
	})

	b.Run("UnmarshalBinary", func(b *testing.B) {
		var got shard.ID
		var err error

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			err = got.UnmarshalBinary(encoded)
		}

		assert.NoError(b, err, "the benchmark must measure a valid encoding")
		assert.Equal(b, got, recordedID, "the benchmark must measure the decode of the recorded ID")
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
