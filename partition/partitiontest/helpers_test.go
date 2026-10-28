// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package partitiontest_test

import (
	"sync"

	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/shard"
)

const (
	// cell is the cell of the layout of the suite of Layout.
	cell shard.CellID = "c1"
	// largeShards is the number of shards of the layout of the suite of Layout, the
	// most that evenCover builds.
	largeShards = 65536
)

// largeLayout returns the layout of largeShards shards whose Locate the suite of Layout
// measures. It builds the layout on its first call, and every call returns that
// layout.
var largeLayout = sync.OnceValue(func() partition.Layout { return evenCover(largeShards) })

// newLayout returns a copy of the layout of largeLayout, the subject of the suite of
// Layout. The copies share the shards, and Locate does not modify them.
func newLayout() *partition.Layout {
	l := largeLayout()
	return &l
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
