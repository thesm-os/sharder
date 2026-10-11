// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package storetest checks a store.Store against the laws of package store.
//
// [AssertStore] runs one subtest per method of the store, each with the laws that
// constrain the method. A stateful machine runs commits, heads, reads and outcomes of one
// scope on two clients, and checks after every step that the history of its calls is
// linearizable against a model of the scope. The run requires each outcome of a commit
// in a share of its cases, so a run that misses one fails. Properties over generated
// commits check the pages of Read, the events of Watch, the state that Read returns, and
// Fits. Core's epochtest checks that every writer is a fenced writer.
//
// [WithLostCommits], [WithReopen], [WithDelete] and [WithCompact] add the laws that only
// a fault, a restart, the deletion of a scope or the compaction of the history can test.
//
// # Callbacks
//
// A law calls newStore and the functions of the options inside a property case, on a
// goroutine of the case. They receive an assert.TB. Inside a property, that assert.TB is
// the case, so a failure that they report fails the case.
//
// # Concurrency
//
// AssertStore runs its subtests in parallel, and each case opens its own store, so no
// case observes the scopes of another.
//
// # Dependency position
//
// storetest imports the standard library, go.dokimi.dev/assert and its packages expect,
// history, prop and stateful, the packages coretest/epochtest, crypto, crypto/sha256,
// epoch, id, page and version of go.thesmos.sh/core, and the packages budget,
// internal/streamtest, proposal, shard and store of go.thesmos.sh/sharder.
package storetest
