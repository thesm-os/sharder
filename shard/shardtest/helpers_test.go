// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shardtest_test

import (
	"strconv"
	"sync"

	"go.thesmos.sh/sharder/shard"
)

// cells is the number of cells of the cell map of the suite of CellMap.
const cells = 1024

// largeCellMap returns the map of evenCover(cells), whose Locate the suite of CellMap
// measures. It builds the map on its first call, and every call returns that map.
var largeCellMap = sync.OnceValue(func() shard.CellMap { return evenCover(cells) })

// newCellMap returns a copy of the cell map of largeCellMap, the subject of the suite of
// CellMap. The copies share the cells, and Locate does not modify them.
func newCellMap() *shard.CellMap {
	m := largeCellMap()
	return &m
}

// evenCover returns a cell map of n cells, n at most 65,536. Cell i starts at the
// big-endian encoding of i in two bytes, except the first cell, which starts at the empty
// position, and ends where cell i+1 starts.
func evenCover(n int) shard.CellMap {
	cells := make([]shard.Cell, n)
	for i := range n {
		var start, end []byte
		if i > 0 {
			start = []byte{byte(i >> 8), byte(i)}
		}
		if i+1 < n {
			end = []byte{byte((i + 1) >> 8), byte(i + 1)}
		}
		cells[i] = shard.Cell{ID: shard.CellID(strconv.Itoa(i)), Range: shard.Range{Start: start, End: end}}
	}
	return shard.CellMap{Cells: cells}
}
