// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package control runs the controller of one scope of a namespace as a part of a state
// machine.
//
// The root of a namespace places its cells on controller processes. The controller of a
// cell places the shards of the cell on the nodes of the cell. Each of the two runs one
// [Controller] over its scope. The Controller appends the effects of package
// go.thesmos.sh/sharder/internal/machine, and [Controller.Step] takes the events of their
// results. A runtime performs every effect, and a simulation replays a run from the same
// events. The machine of the root and the machine of a cell start their Controller, give it
// the spec that it applies, and stop it.
//
// # Lifecycle
//
// A table of core's fsm declares the states of a Controller and the event of each edge
// between them. [Controller.Acquire] acquires the scope at Config.Term with the mutation
// that Config.Acquisition completes. Once the acquisition has applied, the Controller
// builds its engine and its assignment from the snapshot of the scope, continues every
// transition of the scope, and serves. [Result] reports the end of the acquisition to the
// machine. The Controller watches the views of the membership of its scope from then on.
// [Controller.Stop] and [Controller.Stopped] stop it, and an error stops it too.
//
// # Specs
//
// [Resolve] and [Build] build the [Strategies] of a spec. [Controller.Accept] takes the
// strategies of a newer spec, which take effect at the next pass: the engine takes their
// settings, and the first commit of the pass records their version in the record of the
// controller.
//
// # Passes
//
// A pass is a Plan effect over a copy of the committed assignment. A node that restarts
// restores no writer. The copy leaves out each writer whose node the view has at another
// incarnation than its placement records, and the pass places the shard again. The
// placement passes run one at a time, and a trigger during a pass starts one more pass
// after it. These events start a pass:
//
//   - The first view, and each later view that ends the incarnation of a node with
//     replicas.
//   - A change of the view, when the schedule of the placer sets OnMembership.
//   - The interval of the schedule of the placer, and a newer spec.
//   - A report with a new permanent fault, a commit that leaves a shard without a writer,
//     and a commit that adds or removes a drain.
//   - A commit that ends the transition of a shard whose writer the view does not have at
//     the incarnation of its placement.
//   - A commit that ends the transition of a shard whose writer is on a node that drains.
//   - The end of a transition, while the bounds of the engine refused a move of the last
//     plan.
//
// The input of a pass lists a fault of a shard for each node whose live incarnation reports
// the replica of the shard with a permanent fault, or failed an add of the shard
// definitively. The placer does not place a replica of the shard on such a node. The
// Emergency of the input is true when a shard without a transition in flight has no writer
// on a live node, or has its writer on a node with a fault of the shard.
//
// The Controller seeds the source of randomness of each pass from its placement.Snapshot,
// so a recorded pass replays. It starts the move of each shard of a plan in the order of
// the IDs of the shards. It admits each move first, against one placement.State of the step
// that takes the plan: the committed assignment, every transition of the engine, and the
// moves that the step has admitted. A transition counts on its destination and on its
// fallback there, so the constraints of the spec apply to every outcome of the transitions.
// A move that the admission refuses waits for a later pass. The source of a move has the
// incarnation that its placement records for its writer. The destination has the
// incarnation of its node in the view. The fallback of a move is its source when the view
// has the node of the source at that incarnation, the node does not drain, and its replica
// of the shard has no fault. The
// Controller skips a target on a node that started to drain after the input of the pass. A
// shard without a reported load has Config.Load in a pass, or the mean load of the shards
// with one when Config.Load is nil.
//
// # Budget
//
// The Controller takes the Disrupt calls of the budget of its scope. A new disruption
// commits a pending reservation with a drain of each of its nodes, and the drains move the
// replicas of the drain roles off the nodes. The budget admits a negotiable disruption
// when these conditions are true:
//
//   - Every live node has reported to the Controller since the acquisition.
//   - No replica of the drain roles remains on the nodes of the disruption.
//   - No transition in flight can end with a replica of the drain roles on the nodes.
//   - The admitted disruptions cover at most maxConcurrentNodes nodes with replicas.
//
// A reservation, a drain and an undrain take effect at the step that commits them, because
// the commits apply in the order of their steps. A Done of a reservation whose commit is in
// flight ends the reservation. No move starts toward a node after the step that drains it,
// so the nodes of an admitted negotiable disruption remain empty.
//
// Each deferral has a RetryAfter of [RetryAfter]. The Controller admits a disruption that
// is not negotiable at its Start. A pending reservation ends at the End of its disruption,
// and the pending reservation of a node of itself also ends with the incarnation of the
// node. The Controller ends an admitted reservation at a Done, or once its nodes have left
// the view or restarted and do not host a replica.
//
// A deferred Disrupt whose deadline is more than machine.WaitMargin away waits for its
// pending reservation:
//
//   - After each step that applies a commit, takes a report or takes a view, the
//     Controller evaluates each reservation with a waiting call, in the order of the IDs.
//     One commit admits each negotiable one that the budget allows.
//   - A waiting call gets the admission once the commit that admits its reservation has
//     applied, and the deferral of the end once the commit that ends it has applied.
//   - A Timer at machine.WaitMargin before the deadline of a call replies with the deferral
//     of the reservation and a RetryAfter of zero.
//
// The drain roles of the support matrix are every role of its model, so an admitted
// negotiable disruption does not make a replica unavailable. maxUnavailable bounds the
// moves instead, the moves of the passes and the move proposals alike. A move makes the one
// replica of its shard unavailable, so maxUnavailable admits it above zero. A move of a
// writer that has failed makes nothing unavailable: the view does not have its node at the
// incarnation of its placement, or that node incarnation reports its tenure with a permanent
// fault. Neither does a move off a node that an admitted reservation covers. The budget
// admits both.
//
// # Proposals
//
// An operator submits a proposal of a kind that Config.Kinds lists. The Controller refuses
// a proposal whose Expires has passed, whose ID the scope has applied, or whose Base the
// state has moved past. A drain and an undrain commit the manual drain of their node with
// the record of the proposal. For a move, the Controller runs a pass that checks the target
// against the constraints of the spec, and then admits the move as it admits the moves of
// a pass. A move that the budget refuses fails with [ErrBudget], and a move that the
// admission refuses fails with [ErrStale]. The batch with the first record of the
// transition records the proposal. The pass of an explain returns the cost of each tier of
// the goals and the violations of the replicas of its shard, and the Controller commits
// nothing for it. A spec proposal must propose the version after the accepted spec of the
// namespace. Its spec must resolve, its strategies must build and config.Change must
// accept it, and the Controller then commits the spec with the record of the proposal.
//
// # Writer gaps
//
// The Controller measures each writer gap in sharder.writer.gap. The commit that removes
// the writer of a shard starts a planned gap. A failure gap starts at the view that ends
// the node incarnation of the writer. Each gap keeps its first start. It ends at the first
// accepted report in which the node of the new writer reports the shard as serving. The nodes
// of a scope with Config.Unreported do not report, so there the commit that ends the
// transition of the shard with a writer ends the gap.
//
// # Replies
//
// The Controller replies to a report at once, and to a proposal or a disruption once its
// commit has applied. A deferral with a deadline waits for its reservation, as the section
// Budget states. A call that waits for its reply when the Controller stops gets
// [ErrNotServing].
//
// # Errors
//
// [ErrConfig], [ErrSpec] and [ErrCall] classify as errs.Invalid. [ErrStale], [ErrApplied]
// and [ErrBudget] classify as errs.Conflict. [ErrNotServing] and [ErrBusy] classify as
// errs.Transient, and [ErrDenied] classifies as errs.Denied.
//
// # Concurrency
//
// A Controller is not safe for concurrent use. Its machine calls it during Machine.Step.
//
// # Dependency position
//
// control imports the standard library, the packages btree, clock, crypto, crypto/sha256,
// epoch, errs, fsm, id, id/ulid, idmap, rand, rand/pcg, resilience and telemetry of
// go.thesmos.sh/core, and the packages budget, config, handoff, internal/engine,
// internal/machine, internal/scope, membership, ownership, partition, placement,
// placement/planner, proposal, protocol, replication, shard and store of
// go.thesmos.sh/sharder.
package control
