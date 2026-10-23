// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package router maps the key of each request of an application to the replicas of its shard.
//
// A client process embeds a router to route its requests. [New] starts a [Watcher] for one
// namespace. A Watcher routes in watch mode, and the support matrix has no other routing
// source. The Watcher reads the routes of each cell from the store and the address of each
// node from the views of the membership. [Watcher.Route] then returns the route of a key
// without a wait.
//
// # Routes
//
// The partitioner of the namespace maps a key to its position, and the cell map maps the
// position to its cell. The cache of the cell has the route of each shard. A [Route] lists an
// [Endpoint] for each replica whose node the latest view of the cell lists. A shard without a
// placement has no route, and neither has a shard whose nodes the view does not list. Within
// one namespace incarnation, a route never goes back to an older generation of its shard.
//
// # Redirects
//
// A server that does not serve the shard of a request replies with a [NotOwnerError] before
// it starts the request. [Do] then waits with [Watcher.Refresh] until the router has applied
// the generation of the cell that the server has observed, and routes the key again. A
// redirect of another namespace incarnation makes the router read the namespace again. Do
// returns every other error of the call at once, because a request that failed after it
// started may have taken effect.
//
// # End of the run
//
// The router runs until [Watcher.Close], or until a read fails with an error that a retry
// does not fix. [Watcher.Done] is closed once the run has ended and every goroutine of the
// router has returned. [Watcher.Err] then returns the error that ended the run, or nil after
// a Close.
//
// # Errors
//
// [ErrConfig] and [ErrClosed] classify as errs.Invalid, [ErrNoRoute] as errs.Transient,
// [ErrMapping] as errs.Unsupported and [ErrCell] as errs.NotFound. A [NotOwnerError]
// classifies as errs.Conflict.
//
// # Concurrency
//
// A Watcher is safe for concurrent use, and so is Do.
//
// # Allocation contract
//
// [Watcher.Route] does not allocate. [Do] does not allocate apart from the allocations of the
// call of the application. Do takes the requests of its calls of the router from a pool.
//
// # Dependency position
//
// router imports the standard library, the packages clock, epoch, errs, pool, rand and
// telemetry of go.thesmos.sh/core, and the packages config, internal/driver,
// internal/machine, internal/machine/routes, membership, shard and store of
// go.thesmos.sh/sharder.
//
//dokimi:sentinel
package router
