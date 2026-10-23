// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package membershiptest checks a membership.Membership against the laws of package
// membership.
//
// [AssertMembership] runs one subtest per method of the membership, each with the laws that
// constrain the method. The leases run on a fake clock that the suite advances. A stateful
// machine joins, stops and removes the nodes of a scope and advances the clock, and checks
// after every step that the current view lists the node of each session whose lease is
// live, and that the Done channel of each session is closed exactly when its lease is lost.
//
// [WithLostRenewals] adds the laws of renewals that a fault interrupts.
//
// # Errors
//
// [ErrUnknownLoss] classifies as errs.Invalid.
//
// # Concurrency
//
// AssertMembership runs its subtests in parallel, and each case builds its own membership
// and clock, so no case observes the sessions of another.
//
// # Dependency position
//
// membershiptest imports the standard library, go.dokimi.dev/assert and its packages prop
// and stateful, the packages clock, clock/fake, epoch, errs, id and tag of
// go.thesmos.sh/core, and the packages internal/streamtest, membership, ownership, shard
// and store of go.thesmos.sh/sharder.
//
//dokimi:sentinel
package membershiptest
