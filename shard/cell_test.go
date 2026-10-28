// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard_test

import (
	"bytes"
	"encoding/hex"
	"slices"
	"strconv"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/shard"
)

const (
	// recordedCellHex pins the encoding of recordedCell, which the root scope and the
	// scope of every cell persist: field 1 is the ID c1, field 2 the range, field 3
	// the controller's address h:1, field 4 its identity i1, field 5 the epoch 2, and
	// field 6 the spec version 3.
	recordedCellHex = "0a02" + "6331" +
		"1206" + recordedRangeHex +
		"1a03" + "683a31" +
		"2202" + "6931" +
		"2802" +
		"3003"
	// labelPos labels the position that a property generates.
	labelPos = "pos"
	// labelEnds labels the ends of the cells of a generated cover.
	labelEnds = "ends"
	// lastCell is the ID of the last cell of a generated cover.
	lastCell shard.CellID = "last"
)

// recordedCell is the cell whose encoding recordedCellHex pins.
var recordedCell = shard.Cell{
	ID:                 "c1",
	Range:              recordedRange,
	Controller:         "h:1",
	ControllerIdentity: "i1",
	Epoch:              2,
	SpecVersion:        3,
}

// threeCells is a cell map whose cells cover the position space in three ranges:
// below g, from g to below t, and from t on.
var threeCells = shard.CellMap{Namespace: "orders", Cells: []shard.Cell{
	{ID: "c0", Range: shard.Range{End: []byte("g")}},
	{ID: "c1", Range: shard.Range{Start: []byte("g"), End: []byte("t")}},
	{ID: "c2", Range: shard.Range{Start: []byte("t")}},
}}

// TestCell checks the recorded encoding of a Cell, and CellMap.Locate.
func TestCell(t *testing.T) {
	t.Parallel()

	t.Run("AppendBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("appends the recorded encoding of a Cell", func(t *testing.T) {
			t.Parallel()
			c := recordedCell
			got, err := c.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode the Cell")
			assert.Equal(
				t,
				hex.EncodeToString(got),
				recordedCellHex,
				"AppendBinary must write fields 1 to 6 as recorded",
			)
		})
	})

	t.Run("UnmarshalBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the Cell of the recorded encoding", func(t *testing.T) {
			t.Parallel()
			var got shard.Cell
			assert.NoError(t, got.UnmarshalBinary(decodeHex(t, recordedCellHex)),
				"UnmarshalBinary must decode the recorded encoding")
			assert.Equal(t, got, recordedCell, "UnmarshalBinary must return the recorded Cell")
		})
	})

	t.Run("CellMap", func(t *testing.T) {
		t.Parallel()

		t.Run("Locate", func(t *testing.T) {
			t.Parallel()

			tests := []struct {
				name   string
				give   shard.CellMap
				pos    []byte
				want   shard.CellID
				wantOK bool
			}{
				{
					name: "returns the first cell for the empty position",
					give: threeCells, pos: []byte{}, want: "c0", wantOK: true,
				},
				{
					name: "returns the cell whose range starts at the position",
					give: threeCells, pos: []byte("g"), want: "c1", wantOK: true,
				},
				{
					name: "returns the cell whose range contains a position below the next start",
					give: threeCells, pos: []byte("s\xff"), want: "c1", wantOK: true,
				},
				{
					name: "returns the last cell for a position above every start",
					give: threeCells, pos: []byte("zz"), want: "c2", wantOK: true,
				},
				{
					name: "reports false for a position below the first start",
					give: shard.CellMap{Cells: []shard.Cell{{ID: "c1", Range: shard.Range{Start: []byte("b")}}}},
					pos:  []byte("a"),
				},
				{
					name: "reports false for the end of a last range that has an end",
					give: shard.CellMap{Cells: []shard.Cell{recordedCell}},
					pos:  []byte("m"),
				},
				{name: "reports false for the zero CellMap", give: shard.CellMap{}, pos: []byte("a")},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					got, ok := tt.give.Locate(tt.pos)
					expect.Equal(t, ok, tt.wantOK, "Locate must report whether a range contains the position")
					expect.Equal(t, got.ID, tt.want, "Locate must return the cell whose range contains the position")
				})
			}

			t.Run("reports false for a nil CellMap", func(t *testing.T) {
				t.Parallel()
				var m *shard.CellMap
				got, ok := m.Locate([]byte("a"))
				expect.False(t, ok, "Locate must report that a nil CellMap has no cell")
				expect.Equal(t, got, shard.Cell{}, "Locate must return the zero Cell for a nil CellMap")
			})

			t.Run("returns the cell whose range contains the position", func(t *testing.T) {
				t.Parallel()
				prop.ForAll(t, "Locate finds the one cell of a cover whose range contains the position",
					func(c *prop.Case) {
						m := drawCover(c)
						pos := c.Draw(prop.Bytes(prop.MaxSize(4)), labelPos)
						want := slices.IndexFunc(
							m.Cells,
							func(cell shard.Cell) bool { return cell.Range.Contains(pos) },
						)

						got, ok := m.Locate(pos)

						assert.True(c, ok, "a cover must have a cell for every position")
						assert.Equal(
							c,
							got,
							m.Cells[want],
							"Locate must return the cell that a scan of the cells finds",
						)
					})
			})
		})
	})
}

// TestCellAllocs checks the allocation contracts of the construction of a Cell and of a
// CellMap. MaxAllocs counts the allocations of the whole process, so the test does not run
// in parallel.
func TestCellAllocs(t *testing.T) {
	c := recordedCell

	t.Run("constructs a Cell without an allocation", func(t *testing.T) {
		var got shard.Cell
		expect.MaxAllocs(t, func() {
			got = shard.Cell{
				ID: c.ID, Controller: c.Controller, ControllerIdentity: c.ControllerIdentity, Range: c.Range,
				Epoch: c.Epoch, SpecVersion: c.SpecVersion,
			}
		}, 0, "constructing a Cell must not allocate")
		assert.Equal(t, got, recordedCell, "the test must measure the construction of the Cell")
	})

	t.Run("CellMap", func(t *testing.T) {
		t.Run("constructs a CellMap without an allocation", func(t *testing.T) {
			var got shard.CellMap
			expect.MaxAllocs(
				t,
				func() { got = shard.CellMap{Namespace: threeCells.Namespace, Cells: threeCells.Cells} },
				0,
				"constructing a CellMap must not allocate",
			)
			assert.Equal(t, got, threeCells, "the test must measure the construction of the CellMap")
		})
	})
}

// drawCover generates a cell map whose cells cover the position space: the first cell
// starts at the empty position, each cell ends where the next one starts, and the
// last cell has no end.
func drawCover(c *prop.Case) shard.CellMap {
	ends := c.Draw(prop.List(prop.Bytes(prop.MinSize(1), prop.MaxSize(3)), prop.MaxSize(8)), labelEnds)
	slices.SortFunc(ends, bytes.Compare)
	ends = slices.CompactFunc(ends, bytes.Equal)

	cells := make([]shard.Cell, 0, len(ends)+1)
	var start []byte
	for i, end := range ends {
		cells = append(cells, shard.Cell{ID: shard.CellID(strconv.Itoa(i)), Range: shard.Range{Start: start, End: end}})
		start = end
	}

	return shard.CellMap{Cells: append(cells, shard.Cell{ID: lastCell, Range: shard.Range{Start: start}})}
}
