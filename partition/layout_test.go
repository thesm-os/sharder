// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package partition_test

import (
	"bytes"
	"slices"
	"strconv"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/shard"
)

const (
	// labelPos labels the position that a property generates.
	labelPos = "pos"
	// labelEnds labels the ends of the shards of a generated cover.
	labelEnds = "ends"
	// cell is the cell of the layouts of the tests.
	cell shard.CellID = "c1"
)

// threeShards is a layout whose shards cover the position space in three ranges:
// below g, from g to below t, and from t on.
var threeShards = partition.Layout{Cell: cell, Shards: []partition.Shard{
	{Range: shard.Range{End: []byte("g")}, ID: shard.ID{ID128: id.ID128{15: 1}}},
	{Range: shard.Range{Start: []byte("g"), End: []byte("t")}, ID: shard.ID{ID128: id.ID128{15: 2}}},
	{Range: shard.Range{Start: []byte("t")}, ID: shard.ID{ID128: id.ID128{15: 3}}},
}}

// TestLayout checks the zero Shard and Layout.Locate.
func TestLayout(t *testing.T) {
	t.Parallel()

	t.Run("Shard", func(t *testing.T) {
		t.Parallel()

		t.Run("covers every position when it is zero", func(t *testing.T) {
			t.Parallel()
			prop.True(t, func(pos []byte) bool { return partition.Shard{}.Range.Contains(pos) },
				"the zero Shard must cover every position")
		})

		t.Run("has no labels when it is zero", func(t *testing.T) {
			t.Parallel()
			assert.Empty(t, partition.Shard{}.Labels, "the zero Shard must have no labels")
		})

		t.Run("has the zero ID when it is zero", func(t *testing.T) {
			t.Parallel()
			assert.True(t, partition.Shard{}.ID.IsZero(), "the zero Shard must have the zero ID")
		})
	})

	t.Run("Locate", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name   string
			give   partition.Layout
			pos    []byte
			want   shard.ID
			wantOK bool
		}{
			{
				name: "returns the first shard for the empty position",
				give: threeShards, pos: []byte{}, want: threeShards.Shards[0].ID, wantOK: true,
			},
			{
				name: "returns the shard whose range starts at the position",
				give: threeShards, pos: []byte("g"), want: threeShards.Shards[1].ID, wantOK: true,
			},
			{
				name: "returns the shard whose range contains a position below the next start",
				give: threeShards, pos: []byte("s\xff"), want: threeShards.Shards[1].ID, wantOK: true,
			},
			{
				name: "returns the last shard for a position above every start",
				give: threeShards, pos: []byte("zz"), want: threeShards.Shards[2].ID, wantOK: true,
			},
			{
				name: "reports false for a position below the range of the cell",
				give: partition.Layout{Shards: []partition.Shard{threeShards.Shards[1]}},
				pos:  []byte("a"),
			},
			{
				name: "reports false for the end of the range of the cell",
				give: partition.Layout{Shards: []partition.Shard{threeShards.Shards[1]}},
				pos:  []byte("t"),
			},
			{name: "reports false for the zero Layout", give: partition.Layout{}, pos: []byte("a")},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				got, ok := tt.give.Locate(tt.pos)
				expect.Equal(t, ok, tt.wantOK, "Locate must report whether a range contains the position")
				expect.Equal(
					t,
					got,
					tt.want,
					"Locate must return the ID of the shard whose range contains the position",
				)
			})
		}

		t.Run("reports false for a nil Layout", func(t *testing.T) {
			t.Parallel()
			var l *partition.Layout
			got, ok := l.Locate([]byte("a"))
			expect.False(t, ok, "Locate must report that a nil Layout has no shard")
			expect.True(t, got.IsZero(), "Locate must return the zero ID for a nil Layout")
		})

		t.Run("returns the ID of the shard whose range contains the position", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "Locate finds the one shard of a cover whose range contains the position",
				func(c *prop.Case) {
					l := drawCover(c)
					pos := c.Draw(prop.Bytes(prop.MaxSize(4)), labelPos)
					want := slices.IndexFunc(
						l.Shards,
						func(s partition.Shard) bool { return s.Range.Contains(pos) },
					)

					got, ok := l.Locate(pos)

					assert.True(c, ok, "a cover must have a shard for every position")
					assert.Equal(c, got, l.Shards[want].ID, "Locate must return the ID that a scan of the shards finds")
				})
		})
	})
}

// TestLayoutAllocs checks the allocation contracts of Shard and Layout. MaxAllocs
// counts the allocations of the whole process, so the test does not run in parallel.
func TestLayoutAllocs(t *testing.T) {
	s := threeShards.Shards[1]
	pos := []byte("s")

	t.Run("Shard", func(t *testing.T) {
		t.Run("constructs a Shard without an allocation", func(t *testing.T) {
			var got partition.Shard
			expect.MaxAllocs(t, func() { got = partition.Shard{Span: s.Span, ID: s.ID} }, 0,
				"constructing a Shard must not allocate")
			assert.Equal(t, got, s, "the test must measure the construction of the Shard")
		})
	})

	t.Run("constructs a Layout without an allocation", func(t *testing.T) {
		var got partition.Layout
		expect.MaxAllocs(
			t,
			func() { got = partition.Layout{Cell: threeShards.Cell, Shards: threeShards.Shards} },
			0,
			"constructing a Layout must not allocate",
		)
		assert.Equal(t, got, threeShards, "the test must measure the construction of the Layout")
	})

	t.Run("Locate", func(t *testing.T) {
		var got shard.ID
		expect.MaxAllocs(t, func() { got, _ = threeShards.Locate(pos) }, 0, "Locate must not allocate")
		assert.Equal(t, got, s.ID, "the test must measure a position that a shard contains")
	})
}

// BenchmarkLayout reports the cost of Layout.Locate for layouts of 1, 1,024 and 65,536
// shards, and fails when it allocates.
func BenchmarkLayout(b *testing.B) {
	b.Run("Locate", func(b *testing.B) {
		for _, n := range []int{1, 1024, 65536} {
			l := evenCover(n)
			pos := l.Shards[n/2].Range.Start

			b.Run(strconv.Itoa(n), func(b *testing.B) {
				var ok bool

				c := bench.Start(b).MaxAllocs(0)
				defer c.End()

				for c.Loop() {
					_, ok = l.Locate(pos)
				}

				assert.True(b, ok, "the benchmark must measure a position that a shard contains")
			})
		}
	})
}

// drawCover generates a layout whose shards cover the position space. The first shard
// starts at the empty position, each shard ends where the next one starts, and the
// last shard has no end. Shard i has the ID whose last byte is i+1.
func drawCover(c *prop.Case) partition.Layout {
	ends := c.Draw(prop.List(prop.Bytes(prop.MinSize(1), prop.MaxSize(3)), prop.MaxSize(8)), labelEnds)
	slices.SortFunc(ends, bytes.Compare)
	ends = slices.CompactFunc(ends, bytes.Equal)

	shards := make([]partition.Shard, len(ends)+1)
	var start []byte
	for i := range shards {
		var end []byte
		if i < len(ends) {
			end = ends[i]
		}
		shards[i] = partition.Shard{
			Range: shard.Range{Start: start, End: end},
			ID:    shard.ID{ID128: id.ID128{15: byte(i + 1)}},
		}
		start = end
	}
	return partition.Layout{Cell: cell, Shards: shards}
}

// evenCover returns a layout of n shards, n at most 65,536. Shard i starts at the
// big-endian encoding of i in two bytes, except the first shard, which starts at the
// empty position, and ends where shard i+1 starts. Its ID has i+1 in its last three
// bytes.
func evenCover(n int) partition.Layout {
	shards := make([]partition.Shard, n)
	for i := range n {
		var start, end []byte
		if i > 0 {
			start = []byte{byte(i >> 8), byte(i)}
		}
		next := i + 1
		if next < n {
			end = []byte{byte(next >> 8), byte(next)}
		}
		shards[i] = partition.Shard{
			Range: shard.Range{Start: start, End: end},
			ID:    shard.ID{ID128: id.ID128{13: byte(next >> 16), 14: byte(next >> 8), 15: byte(next)}},
		}
	}
	return partition.Layout{Cell: cell, Shards: shards}
}
