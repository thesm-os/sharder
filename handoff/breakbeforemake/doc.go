// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package breakbeforemake provides the handoff protocol break-before-make. Under its
// scripts, the engine stops the writer of a shard on the old node before it starts the
// writer on the new node.
//
// The shard does not have a writer from the drop on the old node until the add on the
// new node completes. That interval is the writer gap of the move. The description of
// [Protocol] lists the steps of its scripts and their abort paths. A [Config] contains
// the timeouts of the callbacks.
//
// # Errors
//
// [ErrConfig] classifies as errs.Invalid. [Protocol.Script] returns an error that wraps
// handoff.ErrTransfer, which classifies as errs.Invalid, for a transfer without a To.
//
// # Concurrency
//
// A [Protocol] is safe for concurrent use.
//
// # Allocation contract
//
// [Protocol.Script] does not allocate for a transfer with a To.
//
// # Dependency position
//
// breakbeforemake imports the standard library, the package errs of
// go.thesmos.sh/core, and the packages handoff and shard of sharder.
//
//dokimi:sentinel
package breakbeforemake
