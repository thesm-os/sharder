// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package placement decides which nodes host the replicas of the shards of a cell.
//
// A [Placer] reads an [Input], the state of one cell, and returns a [Plan]. The plan
// has a [Target] for each shard whose replicas it changes, and for each shard that it
// left with a replica without a feasible node. The planner changes a [State], its
// working copy of the assignment, one [Move] at a time. Each [Constraint] is a hard
// rule that rejects moves, and each [Goal] scores moves with a weight in a tier.
// [Validate] checks a plan before the core commits it, whatever placer returned it.
//
// # Implementations
//
//   - go.thesmos.sh/sharder/placement/planner is the planner, with repair, an emergency
//     mode, a periodic mode and a local search.
//   - go.thesmos.sh/sharder/placement/constraint provides the six built-in constraints.
//   - go.thesmos.sh/sharder/placement/goal provides the seven built-in goals.
//   - go.thesmos.sh/sharder/placement/placementtest checks a placer, a constraint and a
//     goal against the laws of this package.
//
// # Replicas and moves
//
// Each shard has Max replicas in each role of the model of the input. A move adds,
// removes or relocates one replica and keeps its role. A move whose From equals its To
// keeps a replica on its node, and a constraint checks such a move against the state
// with the replica on the node. The built-in constraints and goals read only the
// exported methods of a State, so a constraint or a goal of an application can do what
// a built-in does.
//
// # Determinism
//
// A pass is a function of its input. Each placer visits nodes and shards in the order
// of their IDs. It reads randomness only from Input.Rand, and time only from
// Input.Clock. A [State] returns IDs in ascending order.
//
// # Errors
//
// [ErrInput], [ErrMove], [ErrPlan] and [ErrCost] classify as errs.Invalid. [ErrNoNode]
// classifies as errs.Conflict.
//
// # Concurrency
//
// A Placer, a Constraint and a Goal are safe for concurrent use. The methods of a State
// other than Apply only read it, so many goroutines call them between the moves that
// Apply applies. Input, Plan, Target, Move, Snapshot, InFlight, Metric, Schedule and
// Violation are value types.
//
// # Allocation contract
//
// The methods of a State other than Apply do not allocate. Check of a Constraint and
// Delta of a Goal do not allocate.
//
// # Dependency position
//
// placement imports the standard library, the packages clock, epoch, errs, rand and
// tag of go.thesmos.sh/core, and the packages partition, replication and shard of
// sharder.
package placement
