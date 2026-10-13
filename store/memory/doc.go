// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package memory is an in-memory store.Store and membership.Membership with hooks that
// inject faults, for tests and simulations. It does not keep anything beyond its
// process, so it does not provide the capability durable.
//
// A [Store] keeps every scope in memory. A scope contains its header, its generations
// since the last [Store.Compact], its receipts, its election and its membership.
// [Store.Membership] returns the [Membership] of the store, which keeps its sessions in
// the scopes of the store. Store and Membership are separate types, because store.Reader
// and membership.Membership declare a Watch method each, with different signatures.
//
// # Time
//
// The leases of the sessions run on the clock that [New] receives. A renewal, a local
// deadline and an expiry each take effect at their due time, in the order of their due
// times. Every call first applies the events that are due on the clock, and a goroutine
// of the store applies them when no call arrives, so a run that advances a fake clock
// gets the same effects whenever the store applies them.
//
// # Faults
//
// [Store.FailAt] loses a commit before it applies or its reply after it applies, and a
// renewal of a lease before or after the store applies it. [Store.StartOutage] makes
// every call fail as during an outage of the backend. [Store.Compact] discards the old
// generations, and [Store.Delete] removes a scope as an operator does before a
// namespace is created again under the same name.
//
// # Errors
//
// [ErrOutage] and [ErrFault] classify as errs.Transient. [ErrClosed] and [ErrToken]
// classify as errs.Invalid. A commit that a fault, an outage, a closed store or its
// context stops returns an error that also wraps version.ErrOutcomeUnknown.
//
// # Concurrency
//
// A [Store] and its [Membership] are safe for concurrent use. One mutex orders every
// call. A value that a call returns shares memory with the store, so the caller does not
// modify it.
//
// # Dependency position
//
// memory imports the standard library, the packages btree, clock, crypto, epoch, errs,
// id, idmap, page and version of go.thesmos.sh/core, and the packages budget, config,
// membership, ownership, proposal, shard and store of go.thesmos.sh/sharder.
package memory
