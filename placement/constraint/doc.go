// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package constraint provides the six built-in constraints of placement.
//
// Each constraint is a placement.Constraint. It reads only the exported methods of
// placement.State, so a constraint of an application can do what a built-in does.
// Check returns one static error for each move that a constraint rejects, so it does
// not allocate.
//
// # Constraints
//
//   - [Capacity] rejects a move that puts a node above a capacity, less a headroom.
//   - [Labels] rejects a replica on a node whose labels fail a [Rule].
//   - [Isolation] rejects a move that puts too many replicas of one shard under one
//     value of a label.
//   - [Drain] rejects a replica on a draining node.
//   - [MaxPerNode] rejects a move that gives a node too many replicas.
//   - [MatchLabel] rejects a replica on a node without the value of a label of its
//     shard, and every replica of a shard without the label.
//
// # Configuration
//
// Each constraint with settings has a configuration type with JSON tags, such as
// [CapacityConfig]. Its Validate method returns every problem of a configuration,
// and the constructor of the constraint refuses a configuration that Validate
// refuses.
//
// # Moves that keep a replica
//
// A move whose From equals its To keeps a replica on its node, and each constraint
// checks the state with the replica on the node. [Capacity] checks such a move against
// the load of the node once the moves of the pass end, and every other move against
// the peak load. A node above its capacity then keeps the replicas that fit, and the
// planner moves the others off it. [MaxPerNode] checks such a move against the replicas
// of the node, and every other move against the peak count, which counts the transitions
// in flight to the node.
//
// # Errors
//
// [ErrConfig] and [ErrUnknownOperator] classify as errs.Invalid. [ErrCapacity],
// [ErrLabels], [ErrIsolation], [ErrDrain], [ErrMaxPerNode] and [ErrMatchLabel] classify
// as errs.Conflict, because a later state can admit the move.
//
// # Concurrency
//
// Every constraint is immutable once its constructor returns it, and safe for
// concurrent use.
//
// # Allocation contract
//
// Check of every constraint does not allocate.
//
// # Dependency position
//
// constraint imports the standard library, the package errs of go.thesmos.sh/core,
// and the packages placement and shard of sharder.
//
//dokimi:sentinel
package constraint
