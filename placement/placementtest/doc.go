// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package placementtest checks a placement.Placer, a placement.Constraint and a
// placement.Goal against the laws of package placement.
//
// An implementation calls [AssertPlacer], [AssertConstraint] or [AssertGoal] in the test
// function Test<Unit>Conformance. The tests of its own behaviour are in other test
// functions, so their subtests and the subtests of the laws have different names. Each
// law is a parallel subtest under the name of the method that it constrains, such as
// Check.
//
// # Inputs
//
// The laws run on the inputs that [Inputs] generates: small cells under
// replication.PrimaryOnly or [Replicated], with accepting, draining and gone nodes,
// labels, loads, transitions in flight and faults. A test configures a constraint or a
// goal with the names of their metrics and labels, such as [CPU] and [Zone]. [Moves]
// lists the moves that a state accepts.
//
// # Laws of a placer
//
//   - Place returns a plan that placement.Validate accepts with the constraints of the
//     placer.
//   - Place returns the same plan for the same input, at every Input.Parallelism, and
//     from concurrent calls.
//   - Place does not change its input.
//   - Place returns a repair cursor that is the cursor of the input or a shard of the
//     layout.
//   - Place returns a target only for a shard that the plan changes or leaves with a
//     deficit. The reason of a deficit wraps placement.ErrNoNode.
//   - Place returns the cause of a context that ended before the pass.
//
// # Laws of a constraint
//
//   - Name returns a name.
//   - Check returns the same error for the same state and move.
//   - Check does not change the state.
//   - Check returns the errors of sequential calls from concurrent calls.
//
// # Laws of a goal
//
//   - Name returns a name.
//   - Cost returns a finite cost, and the same cost for the same state.
//   - Cost does not change the state.
//   - Delta returns the change of Cost for each move of a sequence, and 0 for a move
//     whose From equals its To.
//   - Delta does not change the state.
//   - Delta returns the deltas of sequential calls from concurrent calls.
//
// # Allocations
//
// The laws do not count allocations, because a count covers the whole process and the
// laws run as parallel subtests. [AssertConstraintAllocs] and [AssertGoalAllocs] check
// that Check and Delta do not allocate. An implementation calls them from a test that
// does not run in parallel, such as Test<Unit>Allocs.
//
// # Dependency position
//
// placementtest imports the standard library, go.dokimi.dev/assert with its packages
// history and prop, the packages clock/fake, id, rand, rand/pcg and tag of
// go.thesmos.sh/core, and the packages partition, placement, replication and shard of
// sharder.
package placementtest
