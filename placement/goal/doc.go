// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package goal provides the seven built-in goals of placement.
//
// Each goal is a placement.Goal. It reads only the exported methods of
// placement.State, so a goal of an application can do what a built-in does. Delta
// reads the nodes and the shard of one move and the totals that the state keeps, so
// the planner scores a move without a visit of the cell, and Delta does not allocate.
//
// # Goals
//
//   - The cost of [Count] is the variance of the replica counts of the nodes, weighted
//     by role.
//   - The cost of [Balance] is the variance of the utilisation of one metric.
//   - The cost of [Utilisation] is the sum of the utilisation of one metric above a
//     threshold.
//   - The cost of [Spread] counts the replicas of a shard that share a value of a
//     label.
//   - The cost of [Prefer] counts the replicas outside the value of a label that their
//     shard prefers.
//   - The cost of [Stickiness] counts the replicas that differ from the start of the
//     pass.
//   - The cost of [Colocate] counts the nodes of a group of shards beyond the replicas
//     of one shard.
//
// # Configuration
//
// Each goal with settings has a configuration type with JSON tags, such as
// [BalanceConfig]. Its Validate method returns every problem of a configuration, and
// the constructor of the goal refuses a configuration that Validate refuses.
//
// # Nodes
//
// Count, Balance and Utilisation measure the accepting nodes of Input.Nodes. Spread
// and Prefer read the labels of the nodes that the state describes, so a replica on a
// draining or a gone node has no label.
//
// # Errors
//
// [ErrConfig] classifies as errs.Invalid.
//
// # Concurrency
//
// Every goal is immutable once its constructor returns it, and safe for concurrent
// use.
//
// # Allocation contract
//
// Delta of every goal does not allocate. Cost of Colocate allocates the set of its
// groups, and Cost of every other goal does not allocate.
//
// # Dependency position
//
// goal imports the standard library, the packages errs and tag of go.thesmos.sh/core,
// and the packages placement and shard of sharder.
package goal
