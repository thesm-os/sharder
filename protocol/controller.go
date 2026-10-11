// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package protocol

//go:generate go tool kanon -type=Receipt,DoneArgs

import (
	"context"

	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/shard"
)

// ControllerServer receives the reports of agents, the proposals of operators, and the
// disruptions of operators and cluster managers. Each method serves the scope that its
// message names, calls Metadata.Check first, and does not modify or keep its message.
//
// # Concurrency
//
// A ControllerServer is safe for concurrent use.
type ControllerServer interface {
	// Report accepts one report of an agent.
	Report(ctx context.Context, md Metadata, r *Report) (Ack, error)
	// Submit applies a proposal of a kind that this controller applies, and records
	// md.Caller with it.
	Submit(ctx context.Context, md Metadata, p *proposal.Proposal) (Receipt, error)
	// Disrupt admits a planned disruption under the budget of the cell of its nodes. Every
	// node of d is in one cell.
	Disrupt(ctx context.Context, md Metadata, d *budget.Disruption) (budget.Decision, error)
	// Done ends the reservation a.ID.
	Done(ctx context.Context, md Metadata, a *DoneArgs) error
}

// ControllerClient sends reports, proposals and disruptions to the controller of a cell,
// or to the root when the ID of the cell is empty. The cell of the root is the record of
// the leader of the root in the root scope. Transport adapters implement it, fill the
// metadata of each call, and check that the controller presents the ControllerIdentity of
// the cell.
//
// # Concurrency
//
// A ControllerClient is safe for concurrent use.
type ControllerClient interface {
	// Report sends r to the controller of cell, and returns its ack.
	Report(ctx context.Context, cell *shard.Cell, r *Report) (Ack, error)
	// Submit sends p to the controller of cell, and returns its receipt.
	Submit(ctx context.Context, cell *shard.Cell, p *proposal.Proposal) (Receipt, error)
	// Disrupt sends d to the controller of cell, and returns its decision.
	Disrupt(ctx context.Context, cell *shard.Cell, d *budget.Disruption) (budget.Decision, error)
	// Done sends a to the controller of cell.
	Done(ctx context.Context, cell *shard.Cell, a *DoneArgs) error
}

// Receipt is the reply of a controller to a proposal that it accepted.
//
// The zero Receipt is the reply to a proposal that the controller did not commit.
//
// # Encoding
//
// kanon generates the codec of Receipt. Incarnation is field 1, Applied is field 2,
// Generation is field 3, and Result is field 4.
//
// # Concurrency
//
// A copy of a Receipt shares the bytes of Result with the original.
type Receipt struct {
	// Result is JSON. It contains the warnings of the resolution of a spec, the decision of
	// the budget for a drain, or the constraints and the costs of the goals for an explain
	// proposal.
	Result []byte
	// Generation is the generation of the scope that applied the proposal.
	Generation epoch.Epoch
	// Incarnation is the namespace incarnation that the proposal changed.
	Incarnation shard.NamespaceIncarnation
	// Applied reports whether the controller committed the proposal. The controller does
	// not commit an explain proposal.
	Applied bool
}

// DoneArgs is the argument of Done.
//
// The zero DoneArgs ends no reservation.
//
// # Encoding
//
// kanon generates the codec of DoneArgs. Scope is field 1, Incarnation is field 2, and ID
// is field 3.
//
// # Concurrency
//
// A DoneArgs is a comparable value, and a copy is independent of the original.
type DoneArgs struct {
	// Scope is the scope of the cell of the reservation.
	Scope shard.Scope
	// Incarnation is the namespace incarnation of the reservation.
	Incarnation shard.NamespaceIncarnation
	// ID is the ID of the disruption of the reservation.
	ID budget.ID
}
