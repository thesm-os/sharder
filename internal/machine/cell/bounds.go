// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package cell

import (
	"context"
	"log/slog"
)

// The keys of the attributes of the logs of a cell above a bound.
const (
	// attrCount is the number of nodes or of shards of the cell.
	attrCount = "count"
	// attrMax is the bound that the cell is above.
	attrMax = "max"
)

// above reports for each bound of a cell whether the last check found the cell above it.
type above struct {
	// nodes reports that the view had more nodes than cells.maxNodes.
	nodes bool
	// shards reports that the layout had more shards than cells.maxShards.
	shards bool
}

// bound logs the cell once when the view of its controller rises above cells.maxNodes of
// its spec, and once when its layout rises above cells.maxShards. A cell that falls below a
// bound logs again when it rises above it again. The root does not split a cell, so an
// operator reads the log to decide on a split.
func (m *Machine) bound() {
	ctx := context.Background()
	nodes, shards := m.control.Size()
	cell := slog.String(attrCell, string(m.scopeOf.Cell))
	over := nodes > m.bounds.MaxNodes
	if over && !m.above.nodes {
		m.logger.LogAttrs(ctx, slog.LevelWarn, "cell: the cell has more nodes than cells.maxNodes", cell,
			slog.Int(attrCount, nodes), slog.Int(attrMax, m.bounds.MaxNodes))
	}
	m.above.nodes = over
	over = shards > m.bounds.MaxShards
	if over && !m.above.shards {
		m.logger.LogAttrs(ctx, slog.LevelWarn, "cell: the cell has more shards than cells.maxShards", cell,
			slog.Int(attrCount, shards), slog.Int(attrMax, m.bounds.MaxShards))
	}
	m.above.shards = over
}
