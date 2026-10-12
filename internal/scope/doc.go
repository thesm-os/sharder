// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package scope acquires the scope of a controller and commits the mutations of the
// controller to it.
//
// The root of a namespace and the controller of a cell each write one scope of the store.
// A [Scope] is the part of such a machine that writes the scope. It appends the effects of
// package go.thesmos.sh/sharder/internal/machine for the store, takes the events of their
// results in [Scope.Step], and reports to the machine through a [Result]. It does not
// start a goroutine or perform I/O.
//
// # Acquisition
//
// [Scope.Acquire] reads the header of the scope, and refuses a scope whose schema is above
// store.Schema with [ErrSchema]. It then commits the mutation that [Config.Acquisition]
// returns for the header through a writer of the term. The commit expects the generation
// of the header. It advances the watermark of the scope to the term, so every later commit
// of an older term fails with epoch.ErrFenced. When a commit of the replaced controller
// moved the generation first, the acquisition reads the header again and repeats. Once
// the commit has applied, the scope reads every placement in pages, and passes the state
// and the placements to the machine as a [Snapshot]. A compaction during the read starts
// the read again.
//
// The header of a scope before its first commit has no incarnation. The commit of the
// acquisition then creates the scope with the incarnation that Config.Acquisition
// returns. When another writer creates the scope first, the commit fails with
// shard.ErrIncarnation, and the acquisition reads the header again.
//
// # Commits
//
// [Scope.Commit] gives each mutation the next commit ID of the term, and commits the
// mutations one at a time in the order of the calls. Each commit expects the generation
// that the commit before it created, so two commits cannot overlap. Each commit confirms
// the receipts of the commits that applied before it, so the scope keeps the receipt of
// the last commit only.
//
// # Unknown outcomes
//
// A commit whose outcome is unknown runs again after a backoff, with the same ID, the
// same expected generation and the same mutation. The scope retries it until its outcome
// is known, and sends the next commit only after that. The ceiling of the first backoff
// is [RetryBase], and it doubles with each retry up to [RetryMax]. Each attempt with an
// unknown outcome adds one to Config.Unknown.
//
// The store applies the mutation at most once, because it returns the generation of a
// receipt with the digest of the mutation. When the store reports another generation to
// a retry, the scope reads the receipt of the commit with store.Store.Outcome. A receipt
// shows that the commit has applied. When the scope has no receipt of the commit, the
// commit has not applied. It cannot apply later either, because the generation has moved
// past the generation that it expects.
//
// # Lifecycle
//
// A table of core's fsm declares the states of a Scope and the event of each edge between
// them. In each state, the Scope waits for one effect at most: the header, the commit of
// the acquisition, a page of the snapshot, a commit of the machine, or the receipt of a
// commit with an unknown outcome. The Scope stops in a terminal state after an error.
//
// # Errors
//
// The scope stops at the first error that ends the authority of its writer or that a
// retry cannot fix, and reports the error once in [Result.Err]. These errors are
// epoch.ErrFenced, shard.ErrIncarnation, [ErrSchema], [ErrChanged], store.ErrReused,
// store.ErrTooLarge, and an error of a read that errs.Retryable does not report as
// transient. [ErrConfig] and [ErrNotAcquired] classify as errs.Invalid, ErrSchema
// classifies as errs.Unsupported, and ErrChanged classifies as errs.Conflict.
//
// # Concurrency
//
// A Scope is not safe for concurrent use. Its machine calls it during Machine.Step.
//
// # Dependency position
//
// scope imports the standard library, the packages epoch, errs, fsm, page, rand,
// resilience, telemetry and version of go.thesmos.sh/core, and the packages
// internal/machine, shard and store of go.thesmos.sh/sharder.
package scope
