// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard

//go:generate go tool kanon -type=Cell

import (
	"bytes"
	"slices"

	"go.thesmos.sh/core/epoch"
)

// CellID is the identity of a cell of a namespace. A cell is a shard of the root, and
// the root gives the cell the canonical text of that shard's [ID], which
// [go.thesmos.sh/core/id/ulid.Format] returns. The "cell" label of a node contains the
// same text. The empty CellID is no cell, and a [Scope] with an empty CellID is the
// root of a namespace.
type CellID string

// Cell is one entry of the cell map of a namespace, and the record that the
// controller of the cell commits to the scope of the cell when it acquires the scope.
//
// The zero Cell is no cell.
//
// # Encoding
//
// kanon generates the codec of Cell. ID is field 1, Range is field 2, Controller is
// field 3, ControllerIdentity is field 4, Epoch is field 5, and SpecVersion is field
// 6.
//
// # Concurrency
//
// A copy of a Cell shares the bytes of Range with the original.
//
// # Allocation contract
//
// Construction does not allocate.
type Cell struct {
	// ID is the identity of the cell. The record of the root leader in the root scope
	// has an empty ID.
	ID CellID
	// Controller is the address of the controller of the cell, or empty while the
	// cell has no controller.
	Controller string
	// ControllerIdentity is the identity of the controller of the cell. A node
	// accepts the callbacks of the cell only from this identity.
	ControllerIdentity Identity
	// Range is the range of positions that the cell covers.
	Range Range
	// Epoch is the fence epoch of the controller for the keyspace of the cell in the
	// store.
	Epoch epoch.Epoch
	// SpecVersion is the version of the spec of the namespace that the controller of
	// the cell applies. It is at most the version that the root has accepted.
	SpecVersion epoch.Epoch
}

// CellMap is the list of the cells of a namespace in position order. The ranges of
// the cells cover the position space without gaps or overlaps.
//
// The zero CellMap has no cells, so [CellMap.Locate] does not find a cell in it.
//
// # Concurrency
//
// A copy of a CellMap shares the elements of Cells with the original.
//
// # Allocation contract
//
// Construction and [CellMap.Locate] do not allocate.
//
//dokimi:suite
//dokimi:out path=shardtest/
//dokimi:subject CellMap new=newCellMap
type CellMap struct {
	// Namespace is the namespace of the cells.
	Namespace Namespace
	// Cells lists the cells in the order of the starts of their ranges.
	Cells []Cell
	// Version increases with every change of the cell map.
	Version epoch.Epoch
	// Incarnation is the namespace incarnation of the cells.
	Incarnation NamespaceIncarnation
}

// Locate returns the cell whose range contains pos, and reports whether m has one. It
// finds the cell by binary search over the starts of the ranges, so it compares
// O(log n) starts for n cells. It returns the zero Cell and false when no range
// contains pos, which happens only for a map whose ranges leave a gap, and for a nil
// m, which has no cells.
//
// # Allocation contract
//
// Zero alloc.
//
//dokimi:budget allocs=0
func (m *CellMap) Locate(pos []byte) (Cell, bool) {
	if m == nil {
		return Cell{}, false
	}
	i, found := slices.BinarySearchFunc(m.Cells, pos, func(c Cell, pos []byte) int {
		return bytes.Compare(c.Range.Start, pos)
	})
	if !found {
		i--
	}
	if i < 0 || !m.Cells[i].Range.Contains(pos) {
		return Cell{}, false
	}
	return m.Cells[i], true
}
