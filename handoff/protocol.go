// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package handoff

//go:generate go run golang.org/x/tools/cmd/stringer@v0.52.0 -type=Side -linecomment -output=protocol.string_gen.go

import "go.thesmos.sh/sharder/shard"

// Side is one side of a transfer, such as [Fallback]. A step of a script runs on the
// node of its side. It runs at the epoch that the core reserved for that side:
//
//   - [From] at the epoch of the shard before the move.
//   - [To] at the new epoch of the move.
//   - [Fallback] at the abort epoch of the move.
//
// The script of a move then uses each epoch on the node of one side, and it does not use
// an epoch that the core did not reserve.
//
// The zero Side is no side. A [Commit] with the zero Side commits the placement of the
// shard without a writer.
//
// # Encoding
//
// [Side.String] returns the spelling of a Side, such as "fallback", and the form
// "Side(n)" for a value that is not Valid.
//
// # Concurrency
//
// A Side is a comparable value.
//
// # Allocation contract
//
// [Side.Valid], and [Side.String] of a Valid Side, do not allocate. String allocates the
// form of a value that is not Valid.
type Side uint8

const (
	// From is the side of the node that the move takes the replica from.
	From Side = 1 // from
	// To is the side of the node that the move gives the replica to.
	To Side = 2 // to
	// Fallback is the side of the node that becomes the writer on the abort path.
	Fallback Side = 3 // fallback
)

// Valid reports whether s is From, To or Fallback. The zero Side is not.
//
// # Allocation contract
//
// Zero alloc.
func (s Side) Valid() bool {
	return s >= From && s <= Fallback
}

// Transfer is one move of a replica, as the core passes it to a protocol. It contains
// the node of each side of the move. An empty node is a side that the move does not
// have.
//
// The zero Transfer moves no replica.
//
// # Concurrency
//
// A Transfer is a comparable value, and a copy is independent of the original.
type Transfer struct {
	// From is the node that the move takes the replica from. It is empty for a move that
	// only adds a replica, such as the first placement of a shard.
	From shard.NodeID
	// To is the node that the move gives the replica to.
	To shard.NodeID
	// Fallback is the node that becomes the writer on the abort path. The core chooses
	// From, or another node when From drains or has a fault. Fallback is empty when no
	// node qualifies, and the abort path then leaves the shard without a writer.
	Fallback shard.NodeID
}

// Node returns the node of the side s of t. It returns the empty node for a side that t
// does not have, and for a Side that is not Valid.
//
// # Allocation contract
//
// Zero alloc.
func (t Transfer) Node(s Side) shard.NodeID {
	switch s {
	case From:
		return t.From
	case To:
		return t.To
	case Fallback:
		return t.Fallback
	}
	return ""
}

// Protocol turns a transfer into a script. An implementation is deterministic and does
// not perform I/O, so a controller that resumes a transition gets the script that its
// predecessor ran.
//
// # Concurrency
//
// An implementation is safe for concurrent use.
type Protocol interface {
	// Name returns the registered name of the protocol, such as "break-before-make". A
	// transition records it.
	Name() shard.StrategyName
	// Version returns the version of the scripts of the protocol. It changes when Script
	// returns other actions or other sides for a transfer. A transition records it, and
	// a controller resumes the transition only with a protocol of the same name and
	// version.
	Version() uint32
	// Script returns the script of t. A caller can share the script with other callers,
	// so it does not modify the script.
	//
	// Error modes:
	//   - an error that wraps ErrTransfer when the protocol cannot script t.
	Script(t Transfer) (Script, error)
}
