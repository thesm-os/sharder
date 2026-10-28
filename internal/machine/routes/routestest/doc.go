// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package routestest contains the suite of the hot paths of routes.Machine, and the router
// that its checks run on.
//
// [MachineSuite] is the suite of routes.Machine. A client routes each request of its
// application through Route, and the router steps each view of a cell. The budget of the
// suite states that Route does not allocate, for every key that the call generator of Route
// draws. Route admits concurrent calls, so a parallel benchmark measures it as well.
// [ViewsAllocateOnlyMovedRoutes] states that a view of a cell allocates only the route and
// the endpoints of each slot whose node it moves.
//
// # Routers
//
// [Serve] returns a [Served] router of a namespace of one cell over a memory store, as a
// runtime leaves it once it serves. Its cell takes the views of a check through [Viewed].
//
// # Planted defects
//
// The planted defect of ViewsAllocateOnlyMovedRoutes is the double of Machine over a router
// of Serve, whose Step copies the nodes of each view. The test of the suite runs the check
// on its defect, and the check must fail at the assertion whose message is its statement.
//
// # Doubles
//
// [MachineStub] is a recording double of the methods of routes.Machine. Each method of the
// double returns the results of a function, fixed results or an armed fault.
//
// # Errors
//
// A check whose subject does not implement Viewed ends with a fault that wraps
// [ErrUnviewed].
//
// # Dependency position
//
// routestest imports the standard library, go.dokimi.dev/assert with its package prop,
// go.dokimi.dev/conform, go.dokimi.dev/stub, the packages clock/fake, epoch, errs, id,
// id/ulid, rand/seeded and telemetry/noop of go.thesmos.sh/core, and the packages builtin,
// config, internal/machine, internal/machine/routes, membership, partition,
// partition/fixedslots, shard, store and store/memory of sharder.
//
//dokimi:sentinel
package routestest
