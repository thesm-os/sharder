// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard

//go:generate go tool kanon -type=Placement

import "go.thesmos.sh/core/epoch"

// Placement is the desired replica set of one shard, with the epochs and the
// transition count of the shard.
//
// The zero Placement is no placement.
//
// # Encoding
//
// kanon generates the codec of Placement. Shard is field 1, Range is field 2, Epoch is
// field 3, Reserved is field 4, Sequence is field 5, Replicas is field 6, and
// Incarnation is field 7. The codec returns an error that wraps [ErrUnknownRole] for a
// replica whose Role is not Valid.
//
// # Concurrency
//
// A copy of a Placement shares the bytes of Range and the elements of Replicas with
// the original.
//
// # Allocation contract
//
// Construction does not allocate.
type Placement struct {
	// Range is the range of positions that the shard covers.
	Range Range
	// Replicas lists the assigned replicas. It is empty for an unplaced shard, such as
	// one whose transition ended without a writer. A process can still act as the
	// writer of an unplaced shard until a newer tenure acquires the fence of the
	// shard.
	Replicas []Replica
	// Epoch is the fence epoch of the assigned writer of the shard. It is epoch.Zero
	// while the shard has no assigned writer, and fenced storage admits a zero epoch
	// without fencing it.
	Epoch epoch.Epoch
	// Incarnation is the incarnation of the node of the assigned writer when the writer
	// joined the placement. It is zero while the shard has no assigned writer. A node
	// that restarts has a new incarnation, and the new incarnation restores no writer.
	Incarnation epoch.Epoch
	// Reserved is the highest epoch that any transition of the shard has reserved. It
	// includes an epoch that an abort path reserved and that a callback did not use.
	// The core reserves every later epoch above it, so it never grants an epoch twice.
	Reserved epoch.Epoch
	// Sequence is the Sequence of the latest transition of the shard. It remains
	// after the transition ends, so the next transition takes the number after it.
	Sequence uint64
	// Shard is the shard that the placement places.
	Shard ID
}

// Assignment is the desired placement of every shard of one cell at one generation.
//
// The zero Assignment is the empty assignment of no scope.
//
// # Concurrency
//
// A copy of an Assignment shares the map of placements with the original. A writer
// of the map excludes every reader.
//
// # Allocation contract
//
// Construction does not allocate, and a lookup in Placements does not allocate.
type Assignment struct {
	// Placements maps each shard of the cell to its placement.
	Placements map[ID]Placement
	// Scope is the scope of the cell.
	Scope Scope
	// Generation is the generation of the scope that the assignment is at. It
	// increases by one with every committed change of the assignment.
	Generation epoch.Epoch
}
