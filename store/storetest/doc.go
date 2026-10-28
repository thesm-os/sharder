// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package storetest contains the conformance suite of the stores of package store, and the
// doubles of the interfaces of package store.
//
// [RunStoreSuite] applies every check of [StoreSuite] to a new store of a factory, one store
// for each check and for each case of a property check. The checks state the laws of the
// methods of a store:
//
//   - Head returns the zero Head of a scope before its first commit.
//   - Read returns the zero State of such a scope, the state that generated commits leave,
//     and pages that list each selected placement once at the generation of the read. It
//     returns shard.ErrIncarnation for a scope of another incarnation.
//   - Watch yields each later generation once with its whole mutation, each change that
//     concerns a node, and each commit after its start. It ends with its context, and
//     yields shard.ErrIncarnation for a scope of another incarnation.
//   - A machine of commits on two clients checks that Commit, Head, Read and Outcome are
//     linearizable against a model of the scope. Commit leaves the scope unchanged for a
//     mutation that does not encode.
//   - Campaign grants each term to one candidate at a time, above the watermark and the
//     terms before it. A term ends with the context of its campaign, and the next candidate
//     leads once the leader resigns. Campaign returns the error of its context while another
//     candidate leads.
//   - Fits returns store.ErrTooLarge for exactly the mutations that Commit refuses as too
//     large, near the limits of the store.
//   - Close ends the watches and the terms of the store.
//
// The suite commits mutations of up to 3 placements, 3 transitions, 3 reservations and 3
// drains, so the limits of the store admit them.
//
// # Operations of the kit
//
// A check that needs an operation that store.Store does not declare reads it from an
// interface of the kit that the store implements. [Faulty] loses a commit, [Deleter]
// deletes a scope, and [Compactor] discards the old generations of the scopes. A check whose
// store does not implement its interface ends with a fault that wraps [ErrOperation], and an
// application excuses such a check with conform.Excuse and a reason. The check of restarts
// opens the store again through the restart that conform.Restartable supplies.
//
// # Planted defects
//
// Each check has a planted defect: the double of store.Store over a memory store of package
// store/memory, with the calls that break the law of the check replaced. The test of the
// suite runs each check on its defect, and the check must fail at the assertion whose
// message is its statement. Every other check must pass on the defect.
//
// # Doubles
//
// [StoreStub], [ReaderStub], [WriterStub] and [LeadershipStub] are recording doubles of
// store.Store, store.Reader, store.Writer and store.Leadership. Each method of a double
// returns the results of a function, fixed results or an armed fault.
//
// # Errors
//
// [ErrUnknownLoss] classifies as errs.Invalid, and [ErrOperation] classifies as
// errs.Unsupported.
//
// # Concurrency
//
// The checks run in parallel, and each check and each case opens a store of its own, so no
// check observes the scopes of another.
//
// # Dependency position
//
// storetest imports the standard library, go.dokimi.dev/assert and its packages expect,
// history, prop and stateful, go.dokimi.dev/conform, go.dokimi.dev/stub, the packages
// clock/fake, crypto, crypto/sha256, epoch, errs, id, page and version of
// go.thesmos.sh/core, and the packages budget, internal/streamtest, proposal, shard, store
// and store/memory of go.thesmos.sh/sharder.
//
//dokimi:sentinel
package storetest
