// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placement

import (
	"context"
	"time"

	"go.thesmos.sh/sharder/shard"
)

// Placer proposes the next assignment of a cell. A pass of a placer is a function of
// its input. The same binary returns the same Plan for the same Input at every degree
// of parallelism. Only a pass that ends at its timeout can return another Plan.
//
// # Concurrency
//
// A Placer is safe for concurrent use. The controller of each cell runs the passes of
// the cell one at a time, so cells place in parallel.
type Placer interface {
	// Place returns the plan of one pass over in. It does not modify in.
	//
	// Error modes:
	//   - an error that wraps ErrInput when the placer cannot read in.
	//   - an error that wraps ErrCost when a goal returns a cost that is not finite.
	//   - an error that wraps the cause of ctx when ctx ends before the pass does.
	Place(ctx context.Context, in *Input) (Plan, error)
	// Schedule returns when the controller runs the passes of the placer.
	Schedule() Schedule
}

// Schedule states when the controller of a cell runs the passes of a placer, besides
// the passes that repair work starts.
//
// With the zero Schedule, the controller does not run periodic passes, and a change of
// the membership does not start a pass.
//
// # Concurrency
//
// A Schedule is a comparable value.
type Schedule struct {
	// Interval is the time between two periodic passes. A periodic pass does not run
	// when Interval is zero.
	Interval time.Duration
	// OnMembership reports whether a change of the membership of the cell starts a
	// pass.
	OnMembership bool
}

// Plan is the proposal of a placer for the next assignment of a cell. The core does
// not change the replicas of a shard that Targets does not list.
//
// The zero Plan does not change any shard.
//
// # Concurrency
//
// A copy of a Plan shares the map of Targets and the elements of Costs with the
// original.
type Plan struct {
	// Targets maps each shard whose replicas the plan changes, and each shard that the
	// pass left with a deficit, to its target.
	Targets map[shard.ID]Target
	// Costs lists the cost of each tier of the goals of the placer for the assignment
	// that the plan leaves, in ascending tier order. Lower is better. A placer without
	// goals returns no costs.
	Costs []float64
	// RepairNext is the repair cursor for the next pass. It is the ID of the shard
	// that follows the last shard that the pass examined for repair, in the rotation
	// over the IDs of the layout. It equals Input.RepairFrom when the pass examined no
	// shard for repair.
	RepairNext shard.ID
	// Incomplete reports that the pass ended on its timeout or on a work limit before
	// its search finished. The core validates and commits an incomplete plan like any
	// other, and never reports it as converged.
	Incomplete bool
}

// Target is the proposal of a placer for one shard. The shard has the replicas of
// Replicas and Deficit more.
//
// The zero Target has no replicas and no deficit.
//
// # Concurrency
//
// A copy of a Target shares the elements of Replicas with the original.
type Target struct {
	// Reason explains a deficit. It wraps ErrNoNode and the errors of the constraints
	// that rejected the nodes. It is nil for a target without a deficit.
	Reason error
	// Replicas lists the desired replicas that have a feasible node. The core assigns
	// their epochs.
	Replicas []shard.Replica
	// Deficit counts the desired replicas without a feasible node. The core keeps
	// that many of the current replicas of the shard that Replicas does not list, so a
	// target with a deficit repairs what it can and leaves the rest in place.
	Deficit int
}
