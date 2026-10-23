// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package routes runs the router of one namespace in watch mode as a state machine.
//
// A router maps the key of a request to the replicas of its shard. [Machine] reads the
// namespace from the store and from the membership, and publishes a table of routes that
// [Machine.Route] reads on the goroutines of the application. The Machine appends the
// effects of package go.thesmos.sh/sharder/internal/machine, and [Machine.Step] takes the
// events of their results. It does not start a goroutine or perform I/O.
//
// # Reads
//
// A read of the namespace starts with the root scope. The root scope has the spec, the
// mapping that the root recorded, and the cell map: one placement per cell. The Machine
// builds the partitioner of the spec with Config.Registry, and refuses a namespace whose
// partitioner does not compute the recorded mapping. It then reads the scope of every cell
// of the cell map with a follower, and watches the views of every cell. A cell that the
// root adds to the cell map gets its reads at once. The Machine replies to Started once
// every cell has its first snapshot and its first view.
//
// # Routes
//
// The partitioner divides the range of each cell into slots, one shard per slot. The cache
// of a cell has one entry per slot, and each entry is an atomic pointer to the route of its
// slot. Route computes the position of a key, finds its cell in the cell map, and loads the
// entry of the slot of the position. A route lists an endpoint for each replica whose node
// the latest view of the cell lists, with the address of the node in that view. A shard
// without a placement, and a shard whose nodes the view does not list, has no route.
//
// A snapshot of a cell publishes a new table, and so does a change of the cell map. A
// generation of a cell replaces the route of each placement that it puts. A view replaces
// the route of each slot with a replica on a node that the view adds, removes or gives
// another address, and every other route remains published. The follower passes the
// generations of a cell in order, so a route never goes back to an older generation of its
// shard within an incarnation.
//
// # Instruments
//
// The router counts its lookups in sharder.router.lookups, and the routes of the cache of
// each cell in sharder.router.cache.routes. sharder.router.refresh.lag is the highest
// generation of a cell that a Refresh waits for, less the generation that the router has
// applied. A cancelled Refresh does not count.
//
// # Refresh
//
// A client waits with a Refresh for a generation of a cell, after a server has redirected
// it. The Machine replies once it has applied that generation of the cell. A Refresh of a
// cell that the cell map does not have gets [ErrCell].
//
// # Rediscovery
//
// A read that ends with shard.ErrIncarnation reports a namespace that the root has created
// again. The Machine then reads the namespace again, and so it does for a Refresh of another
// incarnation than the installed one. Triggers that arrive while such a read runs share it.
// The installed routes serve until the new read is complete. One publication then installs
// the new cell map and caches, and the Machine ends the reads of the old incarnation. Each
// Refresh of another incarnation than the one installed gets shard.ErrIncarnation. A
// namespace whose positions the router cannot compute after a rediscovery makes Route
// return [ErrMapping].
//
// # Errors
//
// [ErrConfig] and [ErrClosed] classify as errs.Invalid, and [ErrMapping] classifies as
// errs.Unsupported. [ErrNoRoute] classifies as errs.Transient, and [ErrCell] classifies as
// errs.NotFound.
//
// # Concurrency
//
// A Machine is not safe for concurrent use, except Route and Err. Its runtime calls Step
// from one goroutine at a time.
//
// # Dependency position
//
// routes imports the standard library, the packages epoch, errs, fsm, id/ulid, pool, rand,
// resilience and telemetry of go.thesmos.sh/core, and the packages config,
// internal/follow, internal/machine, membership, partition, shard and store of
// go.thesmos.sh/sharder.
//
//dokimi:sentinel
package routes
