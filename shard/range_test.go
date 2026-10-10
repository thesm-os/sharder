// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard_test

import (
	"bytes"
	"encoding/hex"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/shard"
)

const (
	// recordedRangeHex pins the encoding of recordedRange, which every placement and
	// every cell persists: field 1 is the start a, and field 2 the end m.
	recordedRangeHex = "0a01" + "61" +
		"1201" + "6d"
	// labelStart labels the start of a range that a property generates.
	labelStart = "start"
	// labelEnd labels the end of a range that a property generates.
	labelEnd = "end"
)

// recordedRange is the range whose encoding recordedRangeHex pins.
var recordedRange = shard.Range{Start: []byte("a"), End: []byte("m")}

// TestRange checks Contains and the recorded encoding of a Range.
func TestRange(t *testing.T) {
	t.Parallel()

	t.Run("Contains", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			give shard.Range
			pos  []byte
			want bool
		}{
			{name: "reports true for the start of the range", give: recordedRange, pos: []byte("a"), want: true},
			{name: "reports false for the end of the range", give: recordedRange, pos: []byte("m"), want: false},
			{name: "reports false for a position below the start", give: recordedRange, pos: []byte("A"), want: false},
			{
				name: "reports true for a position that extends the start",
				give: recordedRange,
				pos:  []byte("a\x00"),
				want: true,
			},
			{
				name: "reports true for a position that the end extends",
				give: recordedRange,
				pos:  []byte("l\xff\xff"),
				want: true,
			},
			{
				name: "reports true for the empty position in a range whose Start is empty",
				give: shard.Range{End: []byte("m")}, pos: []byte{}, want: true,
			},
			{
				name: "reports true for a position above the start of a range whose End is empty",
				give: shard.Range{Start: []byte("a"), End: []byte{}}, pos: []byte("zz"), want: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(
					t,
					tt.give.Contains(tt.pos),
					tt.want,
					"Contains must report whether the range has the position",
				)
			})
		}

		t.Run("reports true for every position of the zero Range", func(t *testing.T) {
			t.Parallel()
			prop.True(t, func(pos []byte) bool { return shard.Range{}.Contains(pos) },
				"the zero Range must contain every position")
		})

		t.Run("reports true for the start of a range and false for its end", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "a range contains its start and does not contain its end", func(c *prop.Case) {
				start := c.Draw(prop.Bytes(prop.MaxSize(8)), labelStart)
				end := c.Draw(prop.Bytes(prop.MaxSize(8)), labelEnd)
				if bytes.Equal(start, end) {
					end = append(bytes.Clone(end), 0)
				}
				if bytes.Compare(start, end) > 0 {
					start, end = end, start
				}
				r := shard.Range{Start: start, End: end}
				assert.True(c, r.Contains(start), "the range must contain its start")
				assert.False(c, r.Contains(end), "the range must not contain its end")
			})
		})
	})

	t.Run("AppendBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("appends the recorded encoding of a Range", func(t *testing.T) {
			t.Parallel()
			r := recordedRange
			got, err := r.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode the Range")
			assert.Equal(
				t,
				hex.EncodeToString(got),
				recordedRangeHex,
				"AppendBinary must write fields 1 and 2 as recorded",
			)
		})

		t.Run("appends no bytes for a range whose bounds are empty slices", func(t *testing.T) {
			t.Parallel()
			r := shard.Range{Start: []byte{}, End: []byte{}}
			got, err := r.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode the Range")
			assert.Empty(t, got, "AppendBinary must write empty bounds as the zero Range does")
		})
	})

	t.Run("UnmarshalBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the Range of the recorded encoding", func(t *testing.T) {
			t.Parallel()
			var got shard.Range
			assert.NoError(
				t,
				got.UnmarshalBinary(decodeHex(t, recordedRangeHex)),
				"UnmarshalBinary must decode the recorded encoding",
			)
			assert.Equal(t, got, recordedRange, "UnmarshalBinary must return the recorded Range")
		})
	})
}

// TestRangeAllocs checks the allocation contract of Range. MaxAllocs counts the
// allocations of the whole process, so the test does not run in parallel.
func TestRangeAllocs(t *testing.T) {
	start, end := recordedRange.Start, recordedRange.End
	pos := []byte("g")

	t.Run("constructs a Range without an allocation", func(t *testing.T) {
		var got shard.Range
		expect.MaxAllocs(
			t,
			func() { got = shard.Range{Start: start, End: end} },
			0,
			"constructing a Range must not allocate",
		)
		assert.Equal(t, got, recordedRange, "the test must measure the construction of the Range")
	})

	t.Run("Contains", func(t *testing.T) {
		var got bool
		expect.MaxAllocs(t, func() { got = recordedRange.Contains(pos) }, 0, "Contains must not allocate")
		assert.True(t, got, "the test must measure a position that the range contains")
	})
}

// BenchmarkRange reports the cost of Contains, and fails when it allocates.
func BenchmarkRange(b *testing.B) {
	pos := []byte("g")

	b.Run("Contains", func(b *testing.B) {
		var got bool

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			got = recordedRange.Contains(pos)
		}

		assert.True(b, got, "the benchmark must measure a position that the range contains")
	})
}
