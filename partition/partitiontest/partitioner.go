// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package partitiontest

import (
	"bytes"
	"encoding/json"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/shard"
)

// AssertPartitioner checks the laws of partition.Partitioner against p, and fails t
// when p breaks one. It runs each law as a parallel subtest of t. The subtest of a
// law has the name of the method that the law constrains.
func AssertPartitioner(t *testing.T, p partition.Partitioner) {
	t.Helper()

	t.Run("Position", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the same position for the same key", func(t *testing.T) {
			t.Parallel()
			prop.Deterministic(t, func(key []byte) ([]byte, error) { return p.Position(nil, key), nil },
				"Position must return the same position for the same key")
		})

		t.Run("appends the position of the key to dst", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "Position appends the position of the key to dst", func(c *prop.Case) {
				prefix := c.Draw(prop.Bytes(prop.MaxSize(8)), "prefix")
				key := c.Draw(prop.Bytes(prop.MaxSize(64)), "key")
				want := append(bytes.Clone(prefix), p.Position(nil, key)...)
				assert.Equal(c, p.Position(bytes.Clone(prefix), key), want,
					"Position must append the position of the key to dst", assert.EquateEmpty())
			})
		})
	})

	t.Run("Mapping", func(t *testing.T) {
		t.Parallel()

		m := p.Mapping()

		t.Run("returns an equal mapping on every call", func(t *testing.T) {
			t.Parallel()
			assert.Deterministic(t, func(struct{}) (shard.Mapping, error) { return p.Mapping(), nil }, struct{}{},
				"Mapping must return an equal mapping on every call")
		})

		t.Run("returns the name of the partitioner", func(t *testing.T) {
			t.Parallel()
			assert.NotEmpty(t, m.Partitioner, "the mapping must contain the name of the partitioner")
		})

		t.Run("returns the configuration as JSON", func(t *testing.T) {
			t.Parallel()
			assert.True(t, json.Valid(m.Config), "the configuration of the mapping must be JSON")
		})

		t.Run("returns a hash version exactly when it returns the name of a hash", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, m.HashVersion == 0, m.Hash == "",
				"the mapping must contain a hash version exactly when it contains the name of a hash")
		})
	})

	t.Run("Initial", func(t *testing.T) {
		t.Parallel()

		spans, err := p.Initial(shard.Range{})
		assert.NoError(t, err, "Initial must divide the position space")
		assert.NotEmpty(t, spans, "Initial must return a span of the position space")

		t.Run("returns spans that partition the position space", func(t *testing.T) {
			t.Parallel()
			assertPartition(t, spans, shard.Range{})
		})

		t.Run("returns spans that partition a range between the bounds of two spans", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "Initial partitions a range between the bounds of two of its spans", func(c *prop.Case) {
				first := c.Draw(prop.Integer(0, len(spans)-1), "first")
				last := c.Draw(prop.Integer(first, len(spans)-1), "last")
				r := shard.Range{Start: spans[first].Range.Start, End: spans[last].Range.End}
				got, err := p.Initial(r)
				assert.NoError(c, err, "Initial must divide a range between the bounds of two of its spans")
				assertPartition(c, got, r)
			})
		})

		t.Run("returns equal spans on every call", func(t *testing.T) {
			t.Parallel()
			assert.Deterministic(t, p.Initial, shard.Range{}, "Initial must return equal spans on every call")
		})
	})
}

// assertPartition stops tb unless spans partition r, without gaps or overlaps. It
// checks these properties in order:
//
//   - spans is not empty.
//   - The first span starts at the start of r.
//   - The last span ends at the end of r.
//   - Each span ends where the next span starts.
//   - The starts ascend, so no span before the last is empty.
//   - The last span contains its own start, so it is not empty either.
func assertPartition(tb assert.TB, spans []partition.Span, r shard.Range) {
	tb.Helper()

	assert.NotEmpty(tb, spans, "Initial must return a span of the range")
	assert.Equal(tb, spans[0].Range.Start, r.Start, "the first span must start at the start of the range",
		assert.EquateEmpty())
	last := spans[len(spans)-1].Range
	assert.Equal(tb, last.End, r.End, "the last span must end at the end of the range", assert.EquateEmpty())
	assert.Pairwise(tb, spans, func(earlier, later partition.Span) bool {
		return bytes.Equal(earlier.Range.End, later.Range.Start)
	}, "each span must end where the next span starts")
	assert.Pairwise(tb, spans, func(earlier, later partition.Span) bool {
		return bytes.Compare(earlier.Range.Start, later.Range.Start) < 0
	}, "each span must start above the start of the span before it")
	assert.True(tb, last.Contains(last.Start), "the last span must contain its start")
}
