// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package planner provides the planner, the placer of the support matrix.
//
// A pass of the [Planner] builds a placement.State from its input, repairs the
// replicas that need repair, and then improves the assignment one tier of goals at a
// time with a local search. Every constraint filters the moves, and the goals of each
// tier score them. A pass is a function of its input. It visits shards and nodes in
// the order of their IDs, and reads randomness only from Input.Rand on one goroutine.
// It reduces the evaluations of each step in the order of the candidates, so the plan
// does not depend on Input.Parallelism.
//
// # Repair
//
// Repair examines the shards that need repair in the order of their IDs from the
// repair cursor. A replica on a gone node, a replica with a fault, and a missing
// replica need repair in every pass. Outside an emergency, a replica on a draining
// node and a replica that a constraint rejects need repair too. Repair moves each such
// replica to the best feasible node, also when the move raises the cost of a goal. A
// replica without a feasible node remains where it is, and its shard reports a deficit.
// The churn budget does not limit repair, and neither do the triggers.
//
// # Improvement
//
// After the repair of a periodic pass, the pass improves the assignment when the
// triggers report a node at risk. A node is at risk on a load metric or on the replica
// count when the cell is busy on the metric and the load is imbalanced. [Triggers]
// contains the thresholds of both.
//
// For each tier in ascending order, the local search sweeps the accepting nodes by
// pressure. It evaluates the move of each shard of a node to a sample of
// [Search.Sample] nodes, and applies the best move that lowers the cost of the tier
// without raising the cost of a lower tier. It sweeps again until a sweep does not
// apply a move. [Churn] contains the limits of the moves of the improvement.
//
// # Limits
//
// A pass ends at the first of these limits, and returns its plan with Incomplete set:
//
//   - [Search.MaxMoves] applied moves.
//   - [Search.MaxEvaluations] evaluated candidates.
//   - [Search.MaxSourceShards] examined shards.
//   - [Search.Timeout] on Input.Clock.
//
// The pass checks the limits before each shard that it examines.
//
// # Errors
//
// [ErrConfig] classifies as errs.Invalid. Place returns the errors of package
// placement.
//
// # Concurrency
//
// A Planner is safe for concurrent use when its constraints and goals are. A pass
// evaluates the candidates of a step on up to Input.Parallelism goroutines with
// core's task.Each.
//
// # Dependency position
//
// planner imports the standard library, the packages errs, rand and task of
// go.thesmos.sh/core, and the packages placement and shard of sharder.
package planner
