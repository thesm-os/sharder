// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package cell_test

import (
	"strconv"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/config"
)

// The log messages of a cell above a bound.
const (
	logNodes  = "cell: the cell has more nodes than cells.maxNodes"
	logShards = "cell: the cell has more shards than cells.maxShards"
)

// The keys of the attributes of the logs of a cell above a bound.
const (
	attrCountKey = "count"
	attrMaxKey   = "max"
)

// twoNodes is the bound cells.maxNodes of the tests that bound the nodes of a cell.
const twoNodes = 2

// TestBounds checks the logs of a cell above the bounds of its spec.
func TestBounds(t *testing.T) {
	t.Parallel()

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("logs the cell when its view rises above cells.maxNodes", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, bounded(twoNodes, 0))
			h.start(t)
			h.view(t, nodeA, nodeB, nodeC)
			got := h.logs.logged(logNodes)
			assert.Length(t, got, 1, "the cell must log a view above the bound")
			expect.Equal(t, got[0].attrs[attrCellKey], string(cellID), "the log must name the cell")
			expect.Equal(t, got[0].attrs[attrCountKey], strconv.Itoa(3), "the log must contain the nodes of the view")
			expect.Equal(t, got[0].attrs[attrMaxKey], strconv.Itoa(twoNodes), "the log must contain the bound")
		})

		t.Run("logs nothing for a view at cells.maxNodes", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, bounded(twoNodes, 0))
			h.start(t)
			h.view(t, nodeA, nodeB)
			assert.Empty(t, h.logs.logged(logNodes), "the cell must not log a view at the bound")
		})

		t.Run("logs a view above cells.maxNodes once while the cell remains above the bound", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, bounded(twoNodes, 0))
			h.start(t)
			h.view(t, nodeA, nodeB, nodeC)
			h.view(t, nodeA, nodeB, nodeC)
			assert.Length(t, h.logs.logged(logNodes), 1, "the cell must log the rise above the bound once")
		})

		t.Run("logs the cell again when its view rises above cells.maxNodes again", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, bounded(twoNodes, 0))
			h.start(t)
			h.view(t, nodeA, nodeB, nodeC)
			h.view(t, nodeA, nodeB)
			h.view(t, nodeA, nodeB, nodeC)
			assert.Length(t, h.logs.logged(logNodes), 2, "the cell must log each rise above the bound")
		})

		t.Run("logs the cell when its layout has more shards than cells.maxShards", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, bounded(0, shards-1))
			h.start(t)
			got := h.logs.logged(logShards)
			assert.Length(t, got, 1, "the cell must log a layout above the bound")
			expect.Equal(t, got[0].attrs[attrCountKey], strconv.Itoa(shards), "the log must contain the shards")
			expect.Equal(t, got[0].attrs[attrMaxKey], strconv.Itoa(shards-1), "the log must contain the bound")
		})

		t.Run("logs nothing for a layout at cells.maxShards", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, bounded(0, shards))
			h.start(t)
			assert.Empty(t, h.logs.logged(logShards), "the cell must not log a layout at the bound")
		})

		t.Run("logs a layout above cells.maxShards once", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, bounded(0, shards-1))
			h.start(t)
			h.view(t, nodeA, nodeB)
			assert.Length(t, h.logs.logged(logShards), 1, "the cell must log the layout above the bound once")
		})

		t.Run("takes the bounds of a newer spec", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.view(t, nodeA, nodeB, nodeC)
			h.commitSpec(t, firstVersion+1, func(s *config.Spec) { s.Cells = config.CellPolicy{MaxNodes: twoNodes} })
			assert.Length(t, h.logs.logged(logNodes), 1, "the cell must check its view against the newer bound")
		})

		t.Run("keeps its bounds for a newer spec that does not apply", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.view(t, nodeA, nodeB, nodeC)
			h.commitSpec(t, firstVersion+1, func(s *config.Spec) {
				s.Cells = config.CellPolicy{MaxNodes: twoNodes}
				s.Partitioning.Config = []byte(otherSlots)
			})
			assert.Empty(t, h.logs.logged(logNodes), "the cell must keep the bound of the spec that applies")
		})
	})
}

// bounded returns an option whose spec sets cells.maxNodes to maxNodes and cells.maxShards
// to maxShards. A bound of zero takes the default of the bound.
func bounded(maxNodes, maxShards int) option {
	return func(s *setup) { s.spec.Cells = config.CellPolicy{MaxNodes: maxNodes, MaxShards: maxShards} }
}
