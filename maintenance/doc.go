// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package maintenance runs the maintenance of the namespaces of a store for an application
// that embeds sharder.
//
// An application builds the command line or the console of its operators on a [Client].
// [New] returns a Client over a store reader, a membership and a transport to the
// controllers. The process of the Client does not have to host a namespace.
//
// # Calls
//
//   - [Client.Status] reads the cells of a namespace, the version of its accepted spec, the
//     nodes and the replicas of each cell, its transitions, its reservations and its drains.
//     It reads the store and the membership, and does not call a controller.
//   - [Client.Apply] proposes the next version of the spec of a namespace to the leader of
//     the root.
//   - [Client.Drain] and [Client.Undrain] add and remove the manual drain of a node at the
//     controller of its cell.
//   - [Client.Move] moves the writer of a shard to another node of its cell.
//   - [Client.Explain] returns the constraints and the costs of the goals of the placement of
//     a shard.
//   - [Client.Disrupt] requests a disruption of the nodes of a cell from the budget of the
//     cell, and [Client.Done] ends its reservation.
//
// # Proposals
//
// Each call that submits a proposal finds the controller in the record of the scope of the
// proposal. It gives the proposal the base that the controller checks: the version of the
// accepted spec, the incarnation of a node, the epoch of a placement or the generation of a
// cell. A proposal expires [Config.Lifetime] after the time of the clock. A request without a
// proposal ID gets a new ULID. A retry with the ID of an earlier call names the same proposal,
// and a controller applies the proposal of an ID once. The client does not retry a call, so a
// caller that retries reads the status again first.
//
// # Errors
//
// [ErrConfig] classifies as errs.Invalid. [ErrNoNamespace], [ErrNoController], [ErrNoNode]
// and [ErrNoShard] classify as errs.NotFound. Every other error of a call wraps the error of
// the store, of the membership or of the transport, so errs.Classify returns the class of the
// refusal of a controller.
//
// # Concurrency
//
// A Client is safe for concurrent use when the clock and the source of randomness of its
// Config are.
//
// # Dependency position
//
// maintenance imports the standard library, the packages clock, epoch, errs, id/ulid, page
// and rand of go.thesmos.sh/core, and the packages budget, config, membership, proposal,
// protocol, shard and store of go.thesmos.sh/sharder.
//
//dokimi:sentinel
package maintenance
