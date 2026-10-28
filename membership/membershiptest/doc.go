// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package membershiptest contains the conformance suite of the memberships of package
// membership, and the doubles of the interfaces of package membership.
//
// [RunMembershipSuite] applies every check of [MembershipSuite] to a new membership of a
// factory, one membership for each check and for each case of a property check. The checks
// state the laws of the methods of a membership:
//
//   - A machine joins, leaves and stops the renewals of nodes, and advances the clock of
//     the leases. After every step it checks that the current view lists the node of each
//     session whose lease is live, and that the Done of a session is closed exactly when
//     the session lost its lease. At the end it checks that a watch yielded every view in
//     the order of their revisions.
//   - Join returns shard.ErrIncarnation for a scope of another incarnation. It returns an
//     error of the class Invalid for a node without an ID, and for a lease that the
//     configuration refuses. It does not modify the node, and does not keep it.
//   - Watch yields shard.ErrIncarnation for a scope of another incarnation, also when
//     another incarnation creates the scope, and ends with its context.
//   - A session whose renewals or renewal replies a fault loses closes Done at the deadline
//     of its join, and the view lists its node until the expiry of the last renewal that
//     applied.
//
// # Operations of the kit
//
// A check that needs an operation that membership.Membership does not declare reads it
// from an interface of the kit that the membership implements. [Backed] gives the store of
// the membership and advances the clock of its leases, and [Faulty] loses renewals. A check
// whose membership does not implement its interface ends with a fault that wraps
// [ErrOperation], and an application excuses such a check with conform.Excuse and a
// reason.
//
// # Planted defects
//
// Each check has a planted defect: the double of membership.Membership over the membership
// of a memory store of package store/memory, with the calls that break the law of the
// check replaced. The test of the suite runs each check on its defect, and the check must
// fail at the assertion whose message is its statement. Every other check must pass on the
// defect.
//
// # Doubles
//
// [MembershipStub] and [SessionStub] are recording doubles of membership.Membership and
// membership.Session. Each method of a double returns the results of a function, fixed
// results or an armed fault.
//
// # Errors
//
// [ErrUnknownLoss] classifies as errs.Invalid, and [ErrOperation] classifies as
// errs.Unsupported.
//
// # Concurrency
//
// The checks run in parallel, and each check and each case builds a membership of its own,
// so no check observes the sessions of another.
//
// # Dependency position
//
// membershiptest imports the standard library, go.dokimi.dev/assert and its packages prop
// and stateful, go.dokimi.dev/conform, go.dokimi.dev/stub, the packages clock/fake, epoch,
// errs, id and tag of go.thesmos.sh/core, and the packages internal/streamtest,
// membership, ownership, shard, store and store/memory of go.thesmos.sh/sharder.
//
//dokimi:sentinel
package membershiptest
