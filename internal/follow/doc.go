// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package follow reads one scope of the store for a machine, from snapshots and a watch.
//
// The agent and the router each keep the state of a scope in a structure of their own. A
// [Follower] is the part of such a machine that reads the scope. It appends the effects
// of package go.thesmos.sh/sharder/internal/machine for the store, takes the events of
// their results in [Follower.Step], and passes what it reads to the machine through a
// [Result]. It does not keep the state of the scope. It does not start a goroutine or
// perform I/O.
//
// # Snapshots
//
// [Follower.Start] reads a snapshot of the scope in pages, and the follower passes each
// [Page] to the machine as it arrives. Every page of a snapshot is at the generation of
// the snapshot. The follower marks the first page and the last page, so a machine can
// keep its last complete state until the last page of the next snapshot. When the store
// has compacted the generation of a page, the follower reads the snapshot again from its
// first page.
//
// # Watches
//
// After the last page, the follower watches the scope from the generation of the
// snapshot. It passes each later generation to the machine in [Result.Event]. It drops
// every event at or below the generation that it passed last, such as an event that
// arrives again after the watch resumed. A watch of one node also passes the empty events
// that report the progress of the scope. A watch that ends with store.ErrCompacted starts
// a new snapshot. The store ends a watch that way after a compaction, and when the buffer
// of the watcher is full.
//
// # Namespace incarnations
//
// A follower without an incarnation takes the incarnation of its first snapshot. The
// snapshot of a scope before its first commit has no incarnation. The follower then takes
// the incarnation of the first commit that its watch passes, and starts the watch again
// with it. The store checks the incarnation of a watch only when the watch has one, so
// the new watch stops at a namespace that was created again under the same name.
//
// # Lifecycle
//
// A table of core's fsm declares the states of a Follower and the event of each edge
// between them. While it reads a snapshot, the Follower waits for a page or for the Timer
// of a retry. While it watches, it waits for the items of the watch or for the Timer of a
// retry. The Follower stops in a terminal state after Stop or after an error.
//
// # Errors
//
// After a transient error, the follower waits a backoff. It then reads the page again, or
// resumes the watch from the last generation that it passed. The ceiling of the first
// backoff is [RetryBase], and it doubles with each retry up to [RetryMax]. An error that
// is not transient stops the follower, and the follower reports the error once in
// [Result.Err]. shard.ErrIncarnation is not transient. [ErrConfig] classifies as
// errs.Invalid.
//
// # Concurrency
//
// A Follower is not safe for concurrent use. Its machine calls it during Machine.Step.
//
// # Dependency position
//
// follow imports the standard library, the packages epoch, errs, fsm, page, rand and
// resilience of go.thesmos.sh/core, and the packages internal/machine, shard and store of
// go.thesmos.sh/sharder.
package follow
