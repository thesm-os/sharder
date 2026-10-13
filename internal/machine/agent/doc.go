// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package agent runs the agent of one node of a namespace as a state machine.
//
// The agent hosts the replicas that the controller of its cell assigns to the node. It
// checks each callback of the controller before the application sees it, runs the
// callbacks of each shard one at a time through the handler of the application, and keeps
// the fencing token of each tenure of the node. [Machine] is that agent as a machine of
// package go.thesmos.sh/sharder/internal/machine. It appends effects and takes their results
// as events, so a runtime performs every effect and a simulation replays a run.
//
// # Lifecycle
//
// A table of core's fsm declares the states of a Machine and the event of each edge
// between them. After Started, the machine reads the root scope until it has the spec of the
// namespace and the cell of the node. Config.Node.Cell selects the cell, and a node without a
// cell takes the only cell of the namespace. The machine then joins the cell through the
// membership and reads the scope of the cell for the node: its placements, the transitions
// with an endpoint on it, and the record of the controller. It replies to Started once the
// node has a session and the first snapshot is installed. In the support matrix every
// replica is a writer, and a restarted node restores none.
//
// When the session of the node ends, the agent joins again with a new incarnation after a
// backoff. An add of the ended incarnation does not start its tenure. Under ownership.Stop,
// [Machine.Token] returns an error for every tenure from the end of the session on, and an
// add whose fence acquisition completes after the end fails. Under ownership.ContinueFenced,
// the tenures keep their tokens, and the fence rejects their writes once a newer tenure
// acquires. The tenures of an ended incarnation end through cleanups once the controller
// moves their shards. The machine stops when the root creates the namespace again.
//
// # Callbacks
//
// The machine checks each callback in this order before it looks up its key:
//
//   - The metadata of the call and the encoding of the request.
//   - The controller epoch and the caller, against the record of the controller that the
//     watch delivered. A controller epoch above the record gets [ErrUnseen], because the
//     watch delivers the newer record later.
//   - The callback, the role, the path and the epoch. Only the agent builds a request on the
//     path protocol.Cleanup, and every tenure has an epoch above zero.
//   - The scope, the namespace incarnation, the node and the incarnation of the node.
//   - The controller epoch, the epoch of the shard and the operation, against the highest
//     the agent has seen.
//
// A refused callback does not change the state of the machine. The machine raises its
// controller epoch from the record of the controller and from the ack of a report, so a
// callback that passes the checks has that epoch. The machine takes the abort path and the
// end of an operation from the callbacks and from the watch.
//
// The machine keeps the result of each callback of the newest operation of a shard that did
// not end with a transient error. It refuses a key that it has seen with another payload
// with [ErrReused]. When a callback repeats the key of a callback that waits or runs, it
// waits for the same result. An add waits until the watched placement of its shard has the
// node as its writer at the epoch of the add. When its turn comes, it checks the placement
// and the session of its incarnation again. The add then acquires the fence of the tenure,
// and Machine.Token returns the token of the tenure from the acquisition on.
//
// # Cleanups
//
// After each generation and each snapshot of the scope, the machine compares its tenures
// with the watched placements. It queues the cleanup of each tenure that the placements no
// longer list at its epoch: a drop on the path protocol.Cleanup, whose key depends only on
// the tenure. The cleanup runs in the queue of its shard, and only while the tenure is still
// active, so a delayed removal never stops a newer tenure. A transient error runs the
// cleanup again after a backoff.
//
// # Reports
//
// The machine reports every replica of the node to the controller of the cell at these
// times:
//
//   - After each change of a replica.
//   - After the record of a newer controller.
//   - Once per load window of the spec.
//
// One report is in flight at a time, and a report that fails runs again after a backoff.
// The runtime fills the loads of the replicas from the application.
//
// # The agent of the root
//
// With Config.Root, the machine is the agent of a controller process in the root scope. The
// root places the cells of the namespace on controller processes, as the controller of a cell
// places its shards on nodes. The agent of the root checks each callback of the root as the
// agent of a cell checks a callback of its controller. It differs from the agent of a cell in
// three steps:
//
//   - It joins the root scope once the namespace exists, without a cell.
//   - It does not report, because the root refuses reports.
//   - It calls the handler of an add without a fence acquisition. The handler starts the
//     controller of the cell, and the acquisition of the scope of the cell fences every older
//     controller of the cell.
//
// # Close
//
// During a Close, the machine sends the controller a disruption of the node that ends at the
// deadline of the call. Once the replicas of the node have moved away, the controller admits
// the disruption. The machine then ends the session of the node, ends the reservation and
// stops. A Close whose caller stops waiting ends the session without the end of the
// reservation.
//
// # Errors
//
// [ErrConfig], [ErrReused] and [ErrRequest] classify as errs.Invalid. [ErrNotWriter],
// [ErrStale] and [ErrNoFault] classify as errs.Conflict. [ErrNotServing] and [ErrUnseen]
// classify as errs.Transient. [ErrDenied] classifies as errs.Denied, and [ErrNoReplica]
// classifies as errs.NotFound. A callback that the agent does not serve gets an error that
// wraps protocol.ErrUnsupported. Every call that waits when the machine stops gets
// ErrNotServing, and Started gets the error that stopped the machine.
//
// # Concurrency
//
// A Machine is not safe for concurrent use. The runtime calls Step from one goroutine at a
// time. [Machine.Err] and [Machine.Token] are safe for concurrent use.
//
// # Dependency position
//
// agent imports the standard library, the packages btree, clock, crypto, epoch, errs, fsm,
// id/ulid, idmap, rand, resilience and telemetry of go.thesmos.sh/core, and the packages budget,
// config, fence, handoff/breakbeforemake, internal/follow, internal/machine, ownership,
// protocol, replication, shard and store of go.thesmos.sh/sharder.
package agent
