// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package routes

import (
	"context"
	"sync/atomic"

	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/shard"
)

// positionSize is the length of the position of fixed-slots, the longest position of the
// partitioners of the support matrix.
const positionSize = 4

// Endpoint is one replica that a client can call.
//
// # Concurrency
//
// An Endpoint is a value.
type Endpoint struct {
	// Node is the node of the replica.
	Node shard.NodeID
	// Address is the address of the node in the latest view of its cell.
	Address string
	// Role is the role of the replica.
	Role shard.Role
}

// Route lists the replicas of one shard at one epoch. The writer comes first. Each replica of
// the placement whose node the view of the cell lists has an endpoint. The endpoints are in
// the order of the replicas of the placement. In the support matrix, the only replica of a
// placement is its writer.
//
// # Concurrency
//
// A copy of a Route shares the bytes of Range and the elements of Replicas with the
// original. A Route that the router publishes is never modified.
type Route struct {
	// Cell is the cell of the shard.
	Cell shard.CellID
	// Range is the range of positions of the shard.
	Range shard.Range
	// Replicas lists the endpoints of the replicas.
	Replicas []Endpoint
	// Epoch is the epoch of the placement.
	Epoch epoch.Epoch
	// Generation is the generation of the scope of the cell from which the router took the
	// placement: the generation of its snapshot or of the commit that put the placement.
	Generation epoch.Epoch
	// Incarnation is the namespace incarnation of the route.
	Incarnation shard.NamespaceIncarnation
	// Shard is the shard of the route.
	Shard shard.ID
}

// table is the routes of one namespace incarnation that Route reads. The router never
// modifies a table that it has published, except the pointers of its caches.
type table struct {
	// partitioner computes the position of a key.
	partitioner partition.Partitioner
	// err is the error that Route returns for every key, or nil.
	err error
	// caches maps the ID of each cell of cells to the cache of the cell.
	caches map[shard.CellID]*cache
	// cells is the cell map of the namespace.
	cells shard.CellMap
}

// cache is the routes of one cell: one entry per slot of the cell. The position of
// fixed-slots is the number of its slot in 4 bytes in big-endian order, and the empty
// position of singleton is slot 0.
type cache struct {
	// routes lists the route of each slot of the cell, from the slot first on. An entry is
	// nil while its shard has no route.
	routes []atomic.Pointer[Route]
	// first is the slot of the first entry.
	first uint64
}

// Route returns the route of key. It finds the cell of the position of key in the cell map
// of the namespace, and loads the route of the slot of the position from the cache of the
// cell. It reads the table that the router published last, so it does not wait for a step.
// It counts the hit or the miss of key in the instruments of the router with ctx.
//
// Error modes:
//   - ErrNoRoute for a key whose shard has no route, and for every key before the router
//     has read the namespace.
//   - an error that wraps ErrMapping for every key of a namespace whose positions the
//     router cannot compute.
//   - ErrClosed after the router has stopped.
//
// # Concurrency
//
// Route is safe for concurrent use with itself and with Step.
//
// # Allocation contract
//
// Zero alloc. It takes the buffer of the position from a pool.
//
//dokimi:budget allocs=0
//dokimi:mixin concurrent
func (m *Machine) Route(ctx context.Context, key []byte) (Route, error) {
	t := m.published.Load()
	if t.err != nil {
		return Route{}, t.err
	}
	b := m.positions.Get()
	pos := t.partitioner.Position(b[:0], key)
	var r *Route
	if c, ok := t.cells.Locate(pos); ok {
		k := t.caches[c.ID]
		r = k.routes[slot(pos)-k.first].Load()
	}
	m.positions.Put(b)
	if r == nil {
		m.metrics.misses.Add(ctx, 1)
		return Route{}, ErrNoRoute
	}
	m.metrics.hits.Add(ctx, 1)
	return *r, nil
}

// slot returns the number of the slot of the position pos: pos read as a number in
// big-endian order. The empty position is slot 0.
func slot(pos []byte) uint64 {
	var n uint64
	for _, b := range pos {
		n = n<<8 | uint64(b)
	}
	return n
}
