// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package budget

//go:generate go tool kanon -type=Disruption

import (
	"context"
	"time"

	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/shard"
)

// ID is the identity of a disruption in the scope of its cell. The caller assigns it.
// sharderctl uses a ULID, and a cluster manager can use the UUID of its own operation.
//
// The zero ID is no disruption.
//
// # Encoding
//
// An ID encodes through the binary methods of the [id.ID] that it embeds, and the zero
// ID encodes as no bytes.
//
// # Concurrency
//
// An ID is a comparable value. A copy is independent of the original.
type ID struct{ id.ID }

// Disruption is a planned operation on nodes of one cell, such as the restart of a
// process for an upgrade.
//
// The zero Disruption is no disruption.
//
// # Encoding
//
// kanon generates the codec of Disruption. ID is field 1, Scope is field 2,
// Incarnation is field 3, Nodes is field 4, NodeIncarnation is field 5, Impact is
// field 6, Start is field 7, End is field 8, and Negotiable is field 9. The codec
// returns an error that wraps [ErrUnknownImpact] for an Impact other than zero that is
// not valid.
//
// # Concurrency
//
// A copy of a Disruption shares the elements of Nodes with the original.
type Disruption struct {
	// Start is the instant at which the disruption starts.
	Start time.Time
	// End is the instant by which the disruption ends. A pending reservation ends
	// when End passes before its admission.
	End time.Time
	// Scope is the scope of the cell of the nodes.
	Scope shard.Scope
	// Nodes are the nodes that the disruption takes out.
	Nodes []shard.NodeID
	// NodeIncarnation is the incarnation of the node when a node requests a disruption
	// of itself, and zero when an operator requests the disruption. The controller
	// refuses the disruption when the live incarnation of the node differs.
	NodeIncarnation epoch.Epoch
	// ID is unique within the scope. A repeated Admit of the same disruption uses the
	// same ID.
	ID ID
	// Incarnation is the namespace incarnation of the nodes.
	Incarnation shard.NamespaceIncarnation
	// Impact is what the disruption does to the replicas on the nodes.
	Impact Impact
	// Negotiable is false for an operation that starts at Start under any Decision,
	// such as hardware maintenance. The core moves the replicas off the nodes before
	// Start.
	Negotiable bool
}

// Budget admits the planned disruptions of one cell.
//
// # Concurrency
//
// A Budget is safe for concurrent use.
type Budget interface {
	// Admit returns whether d may start now, for the caller with the identity owner.
	// Before it returns a deferral, it commits a pending Reservation of d with the
	// drains of its nodes. Before it returns a Decision that allows d, it commits the
	// admission. For a d whose ID the scope has reserved, Admit evaluates a pending
	// reservation again, and allows an admitted one. Admit does not modify d, and does
	// not keep it after it returns. It returns an error that wraps ErrReusedID for a d
	// whose ID the scope has recorded for another disruption.
	Admit(ctx context.Context, owner shard.Identity, d *Disruption) (Decision, error)
	// Done ends the reservation of id. It releases an admitted reservation, and cancels
	// a pending one together with its drains.
	Done(ctx context.Context, id ID) error
}
