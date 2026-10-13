// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package routes

import (
	"bytes"
	"fmt"
	"sync/atomic"

	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/resilience"

	"go.thesmos.sh/sharder/internal/follow"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// cell is the read of the scope and of the views of one cell, and the routes of its
// shards. The partitioner divides the range of the cell into slots, one shard per slot.
type cell struct {
	// metrics are the instruments of the cell.
	metrics cellMetrics
	// cache is the cache that Route reads, and nil before the first snapshot.
	cache *cache
	// addresses maps each node of the latest view of the cell to its address, and is nil
	// before the first view.
	addresses map[shard.NodeID]string
	// follower reads the scope of the cell.
	follower *follow.Follower
	// scope is the scope of the cell.
	scope shard.Scope
	// spans lists the slots of the cell in slot order, as the partitioner divides its range.
	spans []partition.Span
	// pending lists the placement of each slot of the snapshot that the follower reads.
	pending []entry
	// entries lists the placement of each slot, in the order of spans.
	entries []entry
	// first is the slot of the first span.
	first uint64
	// generation is the last generation of the scope that the router has applied.
	generation epoch.Epoch
	// highest is the highest generation that a Refresh of the cell has waited for.
	highest epoch.Epoch
	// views is the token of the WatchViews that runs, and zero without one.
	views machine.Token
	// wait is the token of the last Timer before a WatchViews.
	wait machine.Token
	// attempt counts the watches of the views that ended with a transient error in a row.
	attempt int
	// count is the number of the routes of the cache.
	count int
}

// entry is the placement of one slot of a cell, and the generation from which the router
// took it. The zero entry is a slot without a placement.
type entry struct {
	// placement is the placement of the shard of the slot.
	placement shard.Placement
	// generation is the generation of the snapshot or of the commit of the placement.
	generation epoch.Epoch
}

// newCell returns the read of the cell l of the discovery d. The partitioner of d divides
// the range of l into its slots.
//
// Error modes:
//   - an error that wraps ErrMapping and the error of partition.Partitioner.Initial for a
//     range that the partitioner does not divide.
//   - an error that wraps the error of follow.New.
func (m *Machine) newCell(d *discovery, l *shard.Cell) (*cell, error) {
	spans, err := d.partitioner.Initial(l.Range)
	if err != nil {
		err = fmt.Errorf("%w: its range: %w", ErrMapping, err)
	}
	scope := shard.Scope{Namespace: m.namespace, Cell: l.ID}
	var f *follow.Follower
	if err == nil {
		f, err = follow.New(&follow.Config{Rand: m.rand, Tokens: &m.tokens, Scope: scope, Incarnation: d.incarnation})
	}
	if err != nil {
		return nil, fmt.Errorf("routes: cell %s: %w", l.ID, err)
	}
	return &cell{
		follower: f,
		spans:    spans,
		metrics:  newCellMetrics(m.reporter, m.namespace, l.ID),
		scope:    scope,
		first:    slot(spans[0].Range.Start),
	}, nil
}

// scoped takes what the follower of the scope of the cell c of the discovery d passed: a
// page of a snapshot, or a later generation of the scope. The last page of a snapshot
// replaces the routes of the cell, and a generation replaces the route of each placement
// that it puts.
//
// Error modes:
//   - the error of lost for the error that stopped the follower.
//   - the error of index for a placement that starts at no slot of the cell.
//   - the error of progress.
func (m *Machine) scoped(d *discovery, c *cell, res *follow.Result, out []machine.Effect) ([]machine.Effect, error) {
	if res.Err != nil {
		return m.lost(d, fmt.Errorf("routes: the scope of %v: %w", c.scope, res.Err), out)
	}
	if p := res.Page; p != nil {
		if p.First {
			c.pending = make([]entry, len(c.spans))
		}
		for i := range p.Placements {
			j, err := c.index(&p.Placements[i])
			if err != nil {
				return out, err
			}
			c.pending[j] = entry{placement: p.Placements[i], generation: p.State.Head.Generation}
		}
		if !p.Last {
			return out, nil
		}
		c.snapshot(d.incarnation, p.State.Head.Generation)
		return m.progress(d, out)
	}
	if res.Event.Generation == 0 {
		return out, nil
	}
	if err := c.apply(d.incarnation, &res.Event); err != nil {
		return out, err
	}
	return m.settle(out), nil
}

// index returns the index of the slot of c at which the placement p starts.
//
// Error modes:
//   - an error that wraps ErrMapping for a placement that starts at no slot of c.
func (c *cell) index(p *shard.Placement) (int, error) {
	j := slot(p.Range.Start) - c.first
	if j >= uint64(len(c.spans)) || !bytes.Equal(c.spans[j].Range.Start, p.Range.Start) {
		return 0, fmt.Errorf("%w: the placement of shard %v in %v starts at %x, where no slot of the cell starts",
			ErrMapping, p.Shard, c.scope, p.Range.Start)
	}
	return int(j), nil
}

// snapshot replaces the placements of c with the pages of the snapshot at the generation gen
// of the namespace incarnation inc, and builds a new cache of their routes.
func (c *cell) snapshot(inc shard.NamespaceIncarnation, gen epoch.Epoch) {
	c.entries, c.generation = c.pending, gen
	c.cache = &cache{routes: make([]atomic.Pointer[Route], len(c.entries)), first: c.first}
	c.count = 0
	for j := range c.entries {
		c.store(j, c.route(inc, &c.entries[j]))
	}
	c.report()
}

// apply applies the generation ev of the scope of c in the namespace incarnation inc. Each
// placement that ev puts replaces the route of its slot.
//
// Error modes:
//   - the error of index for a placement that starts at no slot of c.
func (c *cell) apply(inc shard.NamespaceIncarnation, ev *store.Event) error {
	for i := range ev.Mutation.Put {
		p := &ev.Mutation.Put[i]
		j, err := c.index(p)
		if err != nil {
			return err
		}
		c.entries[j] = entry{placement: *p, generation: ev.Generation}
		c.store(j, c.route(inc, &c.entries[j]))
	}
	c.generation = ev.Generation
	c.report()
	return nil
}

// view takes the view v of the cell in the namespace incarnation inc. It keeps the address
// of each node of v, and replaces the route of each slot with one whose endpoints have
// those addresses.
func (c *cell) view(inc shard.NamespaceIncarnation, v *membership.View) {
	c.addresses = make(map[shard.NodeID]string, len(v.Nodes))
	for i := range v.Nodes {
		c.addresses[v.Nodes[i].ID] = v.Nodes[i].Address
	}
	for j := range c.entries {
		c.store(j, c.route(inc, &c.entries[j]))
	}
	c.report()
}

// route returns the route of the entry e of the namespace incarnation inc. Each replica
// whose node the latest view lists has an endpoint with the address of the node. route
// returns nil when no replica has an endpoint, such as for a slot without a placement.
func (c *cell) route(inc shard.NamespaceIncarnation, e *entry) *Route {
	var endpoints []Endpoint
	for _, r := range e.placement.Replicas {
		if address, ok := c.addresses[r.Node]; ok {
			endpoints = append(endpoints, Endpoint{Node: r.Node, Address: address, Role: r.Role})
		}
	}
	if endpoints == nil {
		return nil
	}
	p := &e.placement
	return &Route{
		Incarnation: inc,
		Cell:        c.scope.Cell,
		Shard:       p.Shard,
		Range:       p.Range,
		Epoch:       p.Epoch,
		Generation:  e.generation,
		Replicas:    endpoints,
	}
}

// store publishes r as the route of the slot j, and counts the routes of the cache.
func (c *cell) store(j int, r *Route) {
	if c.cache.routes[j].Swap(r) != nil {
		c.count--
	}
	if r != nil {
		c.count++
	}
}

// report sets the instruments of c to its routes and its lag.
func (c *cell) report() {
	c.metrics.report(c.count, c.highest, c.generation)
}

// watchViews starts the watch of the views of the cell c of the discovery d.
func (m *Machine) watchViews(d *discovery, c *cell, out []machine.Effect) []machine.Effect {
	c.views = m.next()
	return append(out, machine.WatchViews{Scope: c.scope, Token: c.views, Incarnation: d.incarnation})
}

// viewed takes one item of the watch of the views of a cell. The routes of the cell take the
// addresses of their nodes from a view. After a transient error, the watch runs again once
// a backoff has elapsed. viewed returns out unchanged for an item of a watch that the router
// has ended.
//
// Error modes:
//   - the error of lost for the error of a watch that is not transient.
//   - the error of progress.
func (m *Machine) viewed(ev *machine.Viewed, out []machine.Effect) ([]machine.Effect, error) {
	d, c := m.find(func(c *cell) bool { return c.views == ev.Token })
	if c == nil {
		return out, nil
	}
	if ev.Err != nil {
		c.views = 0
		if !errs.Retryable(ev.Err) {
			return m.lost(d, fmt.Errorf("routes: the views of %v: %w", c.scope, ev.Err), out)
		}
		c.attempt++
		c.wait = m.next()
		return append(
			out,
			machine.Timer{Token: c.wait, Duration: resilience.Backoff(m.rand, c.attempt, RetryBase, RetryMax)},
		), nil
	}
	c.attempt = 0
	c.view(d.incarnation, &ev.View)
	return m.progress(d, out)
}

// elapsed takes the end of the Timer of the backoff of a watch of views, and watches the
// views of its cell again. It returns out unchanged for a Timer that is not the backoff of a
// cell.
func (m *Machine) elapsed(ev machine.Elapsed, out []machine.Effect) []machine.Effect {
	d, c := m.find(func(c *cell) bool { return c.wait == ev.Token })
	if c == nil {
		return out
	}
	return m.watchViews(d, c, out)
}

// find returns the cell of the installed or of the incoming discovery for which match
// reports true, with its discovery. It returns nil for a token of no cell.
func (m *Machine) find(match func(c *cell) bool) (*discovery, *cell) {
	for _, d := range [...]*discovery{m.current, m.incoming} {
		if d == nil {
			continue
		}
		for _, c := range d.cells {
			if match(c) {
				return d, c
			}
		}
	}
	return nil, nil
}
