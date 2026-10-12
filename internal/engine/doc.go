// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package engine runs the transitions of a controller, the moves of the replicas of its
// shards, through the steps of the scripts of a handoff protocol.
//
// The root of a namespace and the controller of a cell each run their transitions through
// one [Engine]. The root moves cells between controller processes, and a cell moves shards
// between nodes. The Engine is a part of the machine of the controller. It appends the
// callbacks and the Timers of package go.thesmos.sh/sharder/internal/machine, and takes the
// events of their results in [Engine.Step]. It returns the changes of the transitions to
// the machine in batches, and the machine commits each batch through its scope. The Engine
// does not start a goroutine or perform I/O.
//
// # Transitions
//
// The machine passes each move to [Engine.Start]. The Engine reserves the epochs and the
// sequence of the move, and scripts the move with its handoff.Protocol. It checks the
// script with handoff.Script.Validate. The first batch of a transition commits its record,
// and the Engine then runs the steps of the script in order. It sends a callback for an
// add or a drop, and a commit step joins the next batch. After each step, the next batch
// commits the next step of the record, so a new controller continues the transition with
// [Engine.Resume].
//
// A step of the script that fails starts the abort path of the step. A step fails after
// its retries, or at once when it fails definitively. A failed step of an abort path runs
// again after [RetryMax], and a definite failure abandons the abort path. An abandoned
// transition ends without an assigned writer. Its last batch commits the placement of the
// shard without the replicas of the nodes of the move.
//
// # Lifecycle
//
// The phase of each transition is a machine of handoff.Lifecycle. The Engine passes these
// events to it:
//
//   - Fail when a step fails.
//   - Finish at the end of a path.
//   - Abandon when a step of an abort path fails definitively.
//
// The wait of each transition is a machine of a table of core's fsm. Each transition waits
// for a batch, a permit of its node, the result of a callback or a Timer.
//
// # Batches
//
// The Engine has at most one batch in flight. The changes that arise while a batch is in
// flight form the next batch, so the transitions share commits. [Engine.Batch] returns the
// next batch, and Config.Fits bounds it. The machine reports with [Engine.Applied] that a
// batch has applied, and the Engine then runs the steps that follow the batch.
//
// # Retries
//
// Each callback has the timeout of its step. A resilience.Retrier decides each retry of a
// callback that fails with a transient error or runs past its timeout. The retry sends the
// same request after a backoff. The Retrier allows [Attempts] attempts of a callback in one
// round. The ceiling of the first backoff is [RetryBase], and the ceiling doubles up to
// [RetryMax]. The budget of the Retrier allows the larger of [MinRetries] retries and
// [RetryBudget] of the callbacks of the Engine within [BudgetWindow]. Any other error fails
// the step definitively.
//
// # Bounds
//
// The Engine starts a transition only while fewer than Config.MaxTransitions transitions
// are in flight. Fewer than Config.MaxNodeTransitions transitions may count against each
// node of the move. A transition counts against the node of its destination and of its
// fallback, and against the node of its source while the session of the source is in the
// view. Each node has Config.NodeCallbacks permits for callbacks. A callback without a
// free permit waits in the first-in, first-out queue of its node.
//
// [Engine.Configure] replaces the protocol, the lock delay and the bounds when a new version
// of the spec takes effect. A transition in flight keeps its script.
//
// [Engine.Inbound] counts the transitions in flight that can end with a replica on a node.
// The disruption budget of the machine admits a disruption of a node only when no
// transition in flight can end with a replica on the node.
//
// # Membership
//
// The machine passes each view of its membership to [Engine.View]. The Engine sends a
// callback only to a node in the view. The incarnation of a node has ended once the view
// has a later incarnation of the node, or once the node has left the view. A drop of an
// ended incarnation completes, and an add on an ended incarnation fails definitively. A
// drop on a node that has left the view completes after Config.LockDelay.
//
// # Errors
//
// [ErrConfig], [ErrMove] and [ErrRecord] classify as errs.Invalid, and [ErrProtocol]
// classifies as errs.Unsupported.
//
// An event that the phase or the wait of a transition refuses is a defect of the Engine.
// The call that sent the event returns an error that wraps fsm.ErrRejected, and the Engine
// is not usable after such an error. A record in a terminal phase also makes
// [Engine.Resume] return such an error.
//
// # Concurrency
//
// An Engine is not safe for concurrent use. Its machine calls it during Machine.Step.
//
// # Dependency position
//
// engine imports the standard library, the packages clock, epoch, errs, fsm, rand and
// resilience of go.thesmos.sh/core, and the packages handoff, internal/machine,
// membership, protocol, shard and store of go.thesmos.sh/sharder.
package engine
