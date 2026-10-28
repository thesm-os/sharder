// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package partition

import (
	"bytes"
	"slices"

	"go.thesmos.sh/sharder/shard"
)

// Shard is a shard of a cell. It has the span that a partitioner proposed, and the ID
// that the core assigned when it committed the first placement of the shard.
//
// The zero Shard covers the whole position space and has no labels and no ID.
//
// # Concurrency
//
// A copy of a Shard shares the bytes of Range and the elements of Labels with the
// original.
//
// # Allocation contract
//
// Construction does not allocate.
type Shard struct {
	Span
	// ID is the identity of the shard.
	ID shard.ID
}

// Layout lists the shards of one cell in position order. The ranges of the shards
// cover the range of the cell without gaps or overlaps.
//
// The zero Layout has no shards, so [Layout.Locate] does not find a shard in it.
//
// # Concurrency
//
// A copy of a Layout shares the elements of Shards with the original.
//
// # Allocation contract
//
// Construction and [Layout.Locate] do not allocate.
//
//dokimi:suite
//dokimi:out path=partitiontest/
//dokimi:subject Layout new=newLayout
type Layout struct {
	// Cell is the cell of the shards.
	Cell shard.CellID
	// Shards lists the shards in the order of the starts of their ranges.
	Shards []Shard
}

// Locate returns the ID of the shard whose range contains pos, and reports whether l
// has one. It finds the shard by binary search over the starts of the ranges, so it
// compares O(log n) starts for n shards. It returns the zero ID and false when no range
// contains pos. No range contains a position outside the range of the cell, and a nil
// l has no shards.
//
// # Allocation contract
//
// Zero alloc.
//
//dokimi:budget allocs=0
func (l *Layout) Locate(pos []byte) (shard.ID, bool) {
	if l == nil {
		return shard.ID{}, false
	}
	i, found := slices.BinarySearchFunc(l.Shards, pos, func(s Shard, pos []byte) int {
		return bytes.Compare(s.Range.Start, pos)
	})
	if !found {
		i--
	}
	if i < 0 || !l.Shards[i].Range.Contains(pos) {
		return shard.ID{}, false
	}
	return l.Shards[i].ID, true
}
