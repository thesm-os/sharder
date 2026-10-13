// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package local connects the servers of the processes of one simulation or test by direct
// calls, without an encoding.
//
// A [Network] contains the agents and the controllers of the processes. Each process calls
// through its own [Local], so every server receives the metadata of the process that calls
// it, as over an authenticating transport. [New] gives one process a network of its own,
// through which a controller calls the agent of its own process.
//
// # Routes
//
// A Local passes a callback to the agent that the network contains for the namespace of the
// request and the node. It passes every other call to the controller process at the
// address in the record of the cell of the call, which [Network.AddController] registers.
// A call follows the record of a cell that the root moves to another process, as over a
// transport adapter that dials that address.
//
// # Errors
//
// A Local returns the error of Metadata.Check before it calls a server, and an error that
// wraps protocol.ErrUnreachable for a call to a server that its network does not contain.
// It returns the same error for a call of a controller whose record has no address. It
// returns the error of a server wrapped, with the class of the error of the server.
//
// # Concurrency
//
// A [Network] and a [Local] are safe for concurrent use. A Local passes the message of the
// caller to the server, so the two share the memory of the message.
//
// # Dependency position
//
// local imports the standard library, and the packages budget, proposal, protocol and
// shard of go.thesmos.sh/sharder.
package local
