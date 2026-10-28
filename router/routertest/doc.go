// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package routertest contains the suite of the hot path router.Watcher.Route, and the double
// of router.Router.
//
// [WatcherSuite] is the suite of router.Watcher. A client process routes each request of
// its application through Route. The budget of the suite states that Route does not
// allocate, for every key that the call generator of Route draws. Route admits concurrent
// calls, so a parallel benchmark measures it as well. Refresh waits until the router has
// applied a generation of a cell, so no benchmark and no budget repeats it.
//
// # Doubles
//
// [RouterStub] is a recording double of [router.Router]. Each method of the double returns
// the results of a function, fixed results or an armed fault.
//
// # Dependency position
//
// routertest imports the standard library, go.dokimi.dev/assert with its package prop,
// go.dokimi.dev/conform, go.dokimi.dev/stub, the packages epoch and id of go.thesmos.sh/core,
// and the package router of sharder.
package routertest
