// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package store

//go:generate go tool kanon -type=Transition,Endpoint

import (
	"time"

	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/shard"
)

// Transition is the durable record of one move of a replica. The core commits it
// before the first callback of the move, and never changes its identity, its endpoints
// or its epochs afterwards, so a new controller resumes the move from the record alone.
// A scope has at most one transition per shard.
//
// The zero Transition is no transition.
//
// # Encoding
//
// kanon generates the codec of Transition. Operation is field 1, Protocol is field 2,
// ProtocolVersion is field 3, SpecVersion is field 4, From is field 5, To is field 6,
// Fallback is field 7, OldEpoch is field 8, NewEpoch is field 9, AbortEpoch is field
// 10, Phase is field 11, Step is field 12, Failed is field 13, and Started is field 14.
// The codec returns an error that wraps shard.ErrUnknownPhase for a Phase other than
// zero that is not valid, and one that wraps shard.ErrUnknownRole for an endpoint whose
// Role is not valid.
//
// # Concurrency
//
// A Transition is a value, and a copy is independent of the original.
type Transition struct {
	// Started is the instant at which the core committed the record.
	Started time.Time
	// Protocol is the registered name of the handoff protocol that scripted the move.
	// A controller resumes the transition only with a protocol of the same name and
	// version.
	Protocol shard.StrategyName
	// Fallback is the node that the abort path makes the writer. It is empty when no
	// node qualifies, and the abort path then ends without an assigned writer.
	Fallback Endpoint
	// From is the node that the move takes the replica from.
	From Endpoint
	// To is the node that the move gives the replica to.
	To Endpoint
	// Operation is the identity of the transition.
	Operation shard.OperationID
	// SpecVersion is the version of the spec under which the core planned the move.
	SpecVersion epoch.Epoch
	// OldEpoch is the epoch of the shard before the move.
	OldEpoch epoch.Epoch
	// NewEpoch is the epoch that the core reserved for the new writer.
	NewEpoch epoch.Epoch
	// AbortEpoch is the epoch that the core reserved for the abort path.
	AbortEpoch epoch.Epoch
	// Step is the next step of the current path: of the script while Phase is
	// shard.PhaseRunning, and of the abort path of the step Failed while Phase is
	// shard.PhaseAborting.
	Step int
	// Failed is the step of the script whose failure started the abort path.
	Failed int
	// ProtocolVersion is the version of the protocol that scripted the move.
	ProtocolVersion uint32
	// Phase is the phase of the transition: shard.PhaseRunning or
	// shard.PhaseAborting, because the commit that ends a transition removes its
	// record.
	Phase shard.Phase
}

// Endpoint is one side of a transition. It contains the node, the incarnation of the
// process of the node that the transition addresses, and the role of the replica on the
// node.
//
// The zero Endpoint is no side. Node is empty for the side of a move that only adds or
// only removes a replica.
//
// # Encoding
//
// kanon generates the codec of Endpoint. Node is field 1, Incarnation is field 2, and
// Role is field 3.
//
// # Concurrency
//
// An Endpoint is a comparable value, and a copy is independent of the original.
type Endpoint struct {
	// Node is the node of the side.
	Node shard.NodeID
	// Incarnation is the incarnation of the process of the node that the callbacks of
	// the transition address.
	Incarnation epoch.Epoch
	// Role is the role of the replica on the node.
	Role shard.Role
}
