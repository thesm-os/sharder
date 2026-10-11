// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package store persists the scopes of namespaces.
//
// A scope is the root of a namespace or one of its cells. It contains the placements, the
// transitions in flight, the reservations and drains of the budget, the applied
// proposals, and the records of the controllers. Its [Head] contains the namespace
// incarnation that created the scope, the generation of the scope and the watermark of
// its fence. A [Writer] commits a [Mutation] as the generation after the one it expects.
// The agent, the router and sharderctl read through a [Reader], and a controller commits
// through a [Store].
//
// # Commits
//
// A commit ends in one of three outcomes:
//
//   - Applied: Commit returns the new generation, and the scope keeps the [Receipt] of
//     the commit.
//   - Rejected: Commit returns one of five definite rejections, and the scope is
//     unchanged.
//   - Unknown: Commit returns an error that wraps version.ErrOutcomeUnknown, and the
//     commit may have applied.
//
// A commit is idempotent by its [CommitID]. A retry of the same mutation under the
// same ID returns the generation that the first attempt created, and does not apply the
// mutation again. Store.Outcome reads a receipt and does not apply anything. An absent
// receipt does not prove a rejection, because the first attempt can still arrive at the
// backend. A controller ends that doubt with a retry, which applies the commit at most
// once, or with a barrier, such as the acquisition of the scope by a newer term.
//
// A later commit of a writer can confirm the receipts of its applied commits. A commit
// that advances the watermark deletes the receipts of the older terms, so a scope keeps
// the receipts of the unconfirmed commits of one writer.
//
// # Acquiring a scope
//
// A controller acquires a scope before it reads the contents of the scope. It reads
// the head, refuses a Head.Schema above [Schema], and commits its record at the
// generation of the head through a writer of its epoch. Once the acquisition has
// applied, every commit of an older term fails with epoch.ErrFenced, and a Read of the
// scope contains every commit that the replaced controller can ever make.
//
// # Snapshots and watches
//
// A reader keeps the state of a scope from a snapshot and a watch:
//
//   - Read returns the state at one generation, and every page of the read is at that
//     generation.
//   - Watch yields every later generation in order, each whole.
//   - A page or a watch at a generation that the store no longer keeps fails with
//     [ErrCompacted]. The reader keeps its last complete state and starts a new
//     snapshot.
//   - A snapshot that replaces the state of a reader does not report a removal. A
//     replica that the old state listed and the snapshot omits was removed while the
//     watch was broken.
//   - A generation is a position in the history of one scope in one namespace
//     incarnation. A reader never compares the generations of two scopes.
//
// # Encoding
//
// kanon generates the binary codecs of [Head], [Transition], [Endpoint], [Drain],
// [DrainKey], [CommitID], [Receipt], [Mutation] and [Applied]. A field keeps its
// number, and a new field takes a new number.
//
// # Errors
//
// [ErrCompacted] classifies as errs.Conflict. [ErrReused] and [ErrTooLarge] classify
// as errs.Invalid.
//
// # Concurrency
//
// Every implementation of [Reader], [Store], [Writer] and [Leadership] is safe for
// concurrent use. A copy of a value of any other type shares its slices, its maps and
// the values of its pointers with the original. A State, a placement or an Event that a
// Reader returns can share memory with the store, so the caller does not modify it.
//
// # Dependency position
//
// store imports the standard library, the packages crypto, crypto/sha256, epoch, errs
// and page of go.thesmos.sh/core, and the packages budget, proposal and shard of
// go.thesmos.sh/sharder. Its generated codecs also import go.thesmos.sh/kanon and
// go.thesmos.sh/kanon/wire.
package store
