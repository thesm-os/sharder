// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package root runs the root of a namespace as a state machine.
//
// The root of a namespace places each cell of the namespace on a controller process, and
// that process runs the controller of the cell. [Machine] is that root as a machine of
// package go.thesmos.sh/sharder/internal/machine. It appends effects and takes their
// results as events, so a runtime performs every effect and a simulation replays a run.
// The controller of each term is a controller of package
// go.thesmos.sh/sharder/internal/control for the root scope.
//
// # Lifecycle
//
// A table of core's fsm declares the states of a Machine and the event of each edge
// between them. One root machine runs in each controller process for the life of the
// process. After Started, the machine campaigns in the election of the root scope, and
// replies to Started. For each term that the election grants, it starts a controller,
// which acquires the root scope at the term. At the end of the term, the machine stops the
// controller and campaigns again. A campaign that ends with a transient error runs again
// after a backoff of [CampaignRetryBase] that doubles up to [CampaignRetryMax]. An error of
// the controller of a term ends the term: the machine logs the error, stops the
// controller and resigns the term. Any other error stops the machine in a terminal state.
//
// # Namespaces
//
// The acquisition of a root scope without a commit creates the namespace in one commit:
// a new namespace incarnation, the record of the root, the mapping and the spec of
// Config.Spec at the first version with every default. When another process creates the
// namespace first, the next attempt of the acquisition takes the namespace of that
// process.
//
// # Cells
//
// The layout of the root has one shard, whose range is every position: the one cell of
// the namespace. The first pass of a controller places the cell and gives it an ID. The
// root applies strategies that a spec of the namespace does not select: primary-only,
// fenced exclusive ownership, break-before-make with its default timeouts, and the
// planner with the capacity constraint and the count goal. Each cell has a load of one on
// the metric [Capacity], and each controller process declares its capacity of that name.
// The views of the root scope list the controller processes, and each process joins the
// root itself. Break-before-make moves a cell between processes with its callbacks add and
// drop. A process does not report to the root, so the writer gap of a cell ends at the
// commit that ends the move of the cell.
//
// # Proposals
//
// The root applies a spec proposal, and its controller refuses every other kind. A
// report, a disruption and the end of a disruption get an error that wraps
// control.ErrCall, because only the controller of a cell takes them. A proposal outside a
// term gets control.ErrNotServing.
//
// # Errors
//
// [ErrConfig] classifies as errs.Invalid.
//
// # Concurrency
//
// A Machine is not safe for concurrent use. A runtime calls Step from one goroutine at a
// time. [Machine.Err] is safe for concurrent use.
//
// # Dependency position
//
// root imports the standard library, the packages clock, epoch, errs, fsm, id/ulid, rand,
// resilience and telemetry of go.thesmos.sh/core, and the packages budget, builtin,
// config, internal/control, internal/machine, placement/constraint, placement/goal,
// placement/planner, proposal, protocol, shard and store of go.thesmos.sh/sharder.
//
//dokimi:sentinel
package root
