// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent

import (
	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/shard"
)

// Observation is what the node has observed of its cell, as Machine.Observed returns it.
type Observation struct {
	// Cell is the cell of the node. It is empty for the agent of the root.
	Cell shard.CellID
	// Generation is the last generation of the scope of the cell that the agent has applied.
	Generation epoch.Epoch
	// Incarnation is the namespace incarnation of the agent.
	Incarnation shard.NamespaceIncarnation
}

// observedCell is the cell and the namespace incarnation that the agent publishes once it
// serves.
type observedCell struct {
	// cell is the cell of the node.
	cell shard.CellID
	// incarnation is the namespace incarnation of the agent.
	incarnation shard.NamespaceIncarnation
}

// Observed returns the observation of the node, as the fields of Observation state it.
// Observed reads the values that the agent publishes, so it does not wait for a Step. The
// generation that it returns does not decrease during the run of the agent.
//
// Error modes:
//   - ErrNotServing before the agent serves, and after it has stopped.
//
// # Concurrency
//
// Observed is safe for concurrent use with itself and with Step.
//
// # Allocation contract
//
// Zero alloc.
func (m *Machine) Observed() (Observation, error) {
	c := m.served.Load()
	if c == nil {
		return Observation{}, ErrNotServing
	}
	return Observation{Cell: c.cell, Generation: epoch.Epoch(m.applied.Load()), Incarnation: c.incarnation}, nil
}
