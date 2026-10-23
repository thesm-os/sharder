// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package replica runs the agent that an application process embeds to host the replicas of
// its shards.
//
// The controller of a cell places each shard of the cell on a node, and the agent of the node
// starts and stops the replicas of the node. [New] returns the agent of a node, which the
// embedder serves through its transport before the agent runs. [Agent.Start] then joins the
// cell of the node and returns once the node has a session and the agent has read the
// placements of the node. The agent runs until the context of Start ends or [Agent.Close]
// returns.
//
// # Handler
//
// The application implements [Handler]. The agent calls AddShard to start a replica and
// DropShard to stop one, one call at a time for each shard. Callbacks arrive at least once.
// The agent refuses a stale callback, and returns its kept result for a repeated key, before
// it calls the handler. Each call of the handler has a deadline of its own. A call that
// returns an error that wraps context.DeadlineExceeded fails its callback with a transient
// error. The agent does not keep such a result. A repeated callback calls the handler again.
// AddShard receives the fencing token of the tenure that it starts. An application that
// reports the load of its replicas also implements [LoadReporter].
//
// Under ownership.Stop, the session of the node ends when the deadline of its lease passes,
// and [Agent.Token] then returns an error for every tenure. A Handler that also implements
// [Revoker] receives a RevokeShard for each tenure of the node, in the turn of its shard, and
// ends the work of the tenure.
//
// # Runner
//
// A [Runner] is a Handler for an application that runs work for each tenure. The application
// implements [Opener]: Open opens a tenure and returns its work, and the runner runs the work
// until the drop or the revocation of the tenure. A replayed add of the tenure that runs
// leaves the work running, a newer tenure of the shard ends the work of the older one, and
// the agent ends the work of every tenure before it closes Done. A work that fails makes the
// runner report a permanent fault of the replica.
//
// # Fencing
//
// Every namespace of the support matrix is exclusive and fenced. The agent acquires the fence
// of each tenure through Config.Guard before it calls AddShard, and [Agent.Token] returns the
// fencing token of each tenure of the node. The application writes each shard through the
// fence with that token.
//
// # Callbacks
//
// [Agent.Do] serves the callbacks of the controller of the cell as a protocol.AgentServer,
// which the embedder serves through a transport adapter. The agent checks the metadata, the
// controller epoch, the caller, the node incarnation, the epochs and the operation of each
// callback before it looks up its key or calls the handler. A callback that starts a tenure
// runs once the placements that the agent watches list the tenure.
//
// # Refusals
//
// A server of the application refuses a request for a shard that its node does not serve
// with a router.NotOwnerError, before it starts the request. [Agent.Observed] returns the
// cell of the node, the last generation of the scope of the cell that the agent has applied,
// and the namespace incarnation, which the refusal contains.
//
// # Faults
//
// [Agent.ReportFault] reports a fault in a replica, and [Agent.Recovered] reports that a
// replica with a transient fault serves again. The agent passes both to the controller in its
// next report.
//
// # End of the run
//
// [Agent.Close] moves the replicas of the node away before the process stops. It sends the
// controller a disruption of the node that ends at the deadline of its context, and leaves
// the cell once the controller admits the disruption. [Agent.Done] is closed once the run of
// the agent has ended and every goroutine of the agent has returned. [Agent.Err] then returns
// the error that ended the run, or nil after a Close.
//
// # Errors
//
// [ErrConfig] and [ErrStarted] classify as errs.Invalid. [ErrNotWriter] and [ErrNoFault]
// classify as errs.Conflict, [ErrNoReplica] as errs.NotFound, and [ErrNotServing] as
// errs.Transient.
//
// # Concurrency
//
// An Agent is safe for concurrent use.
//
// # Dependency position
//
// replica imports the standard library, the packages clock, epoch, errs, pool, rand and
// telemetry of go.thesmos.sh/core, and the packages config, fence, internal/driver,
// internal/machine, internal/machine/agent, membership, protocol, shard and store of
// go.thesmos.sh/sharder.
//
//dokimi:sentinel
package replica
