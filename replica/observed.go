// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package replica

import (
	"context"

	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/shard"
)

// ObservedRequest is the request of Agent.Observed.
type ObservedRequest struct {
	_ incomparable
}

// ObservedResult is the result of Agent.Observed.
type ObservedResult struct {
	_ incomparable
	// Cell is the cell of the node.
	Cell shard.CellID
	// Generation is the last generation of the scope of the cell that the agent has applied.
	Generation epoch.Epoch
	// Incarnation is the namespace incarnation of the agent.
	Incarnation shard.NamespaceIncarnation
}

// Observed returns the cell of the node, the last generation of the scope of the cell that the
// agent has applied, and the namespace incarnation of the agent. A server that refuses a
// request copies them into the router.NotOwnerError of the refusal, with the shard and the
// epoch of the request. The generation that Observed returns does not decrease during the run
// of the agent. Observed reads the values that the agent publishes, so it does not wait for
// the agent, and it does not read ctx.
//
// Error modes:
//   - ErrNotServing before the agent serves, and after its run has ended.
//
// # Concurrency
//
// Observed is safe for concurrent use.
//
// # Allocation contract
//
// Zero alloc. A request in a local variable of the caller remains on the stack of the
// caller.
func (a *Agent) Observed(_ context.Context, _ *ObservedRequest) (ObservedResult, error) {
	o, err := a.machine.Observed()
	//nolint:wrapcheck // Observed returns the sentinel of the agent as it is, so errors.Is matches it without an allocation.
	return ObservedResult{Cell: o.Cell, Generation: o.Generation, Incarnation: o.Incarnation}, err
}
