// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package routes_test

import (
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/history"
	"go.dokimi.dev/assert/prop"
	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/routes"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// The clients of the history of the property of Route, and the bounds of a case.
const (
	// readers is the number of clients that call Route while the router steps.
	readers = 2
	// reads is the most rounds of calls of Route of a reader. A round calls Route once for
	// the key of each slot.
	reads = 100
	// stepClient is the first client of the history that records the steps of the router.
	// The client of slot i is stepClient + i.
	stepClient = 100
	// scriptSize is the most entries of the script of a case.
	scriptSize = 12
	// within is the time that the clients of a case may take.
	within = time.Minute
)

// The names of the operations of the history of the property of Route.
const (
	// stepOperation is a step of the router.
	stepOperation = "step"
	// routeOperation is a call of Route.
	routeOperation = "route"
)

// The kinds of the entries of the script of the property of Route. A kind below scriptView
// places the shard of the slot of the entry at its next epoch. The writer of the placement
// is the element of the kind in scriptWriters.
const (
	// scriptView passes a view of the nodes of the slot of the entry in scriptViews.
	scriptView = 3
	// scriptRecreate creates the namespace again.
	scriptRecreate = 4
	// scriptKinds is the number of kinds.
	scriptKinds = 5
)

// scriptWriters lists the writer of each kind of entry of the script of the property of Route
// that places a shard: node A, node B, and no node.
var scriptWriters = [scriptView]shard.NodeID{nodeA.ID, nodeB.ID, noNode}

// scriptViews lists the nodes of the view of each slot for an entry of the script of the
// property of Route that passes a view: both nodes, node A, node B, and no node.
var scriptViews = [slots][]shard.Node{{nodeA, nodeB}, {nodeA}, {nodeB}, nil}

// routeState is what Route returns for the key of one slot: the route, and whether Route
// returned it without an error.
type routeState struct {
	// route is the route, and the zero Route with an error.
	route routes.Route
	// found is true when Route returned no error.
	found bool
}

// routeSpec is the sequential specification of what Route returns for the key of one slot. A
// step of the router leaves the state that Route returned after it, and a call of Route
// returns the state.
var routeSpec = history.Spec[routeState]{
	Initial: func() routeState { return routeState{} },
	Next: func(s routeState, op history.Operation) []routeState {
		switch op.Name {
		case stepOperation:
			next, _ := op.Output.(routeState)
			return []routeState{next}
		case routeOperation:
			if op.Returned(s) {
				return []routeState{s}
			}
		}
		return nil
	},
}

// TestRoute checks the routes that a router publishes.
func TestRoute(t *testing.T) {
	t.Parallel()

	t.Run("Route", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the route of the shard of a key", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			got, err := h.machine.Route(t.Context(), h.keys[1])
			assert.NoError(t, err, "Route must return the route of a placed shard")
			want := routes.Route{
				Incarnation: incA,
				Cell:        cellID,
				Shard:       shards[1],
				Range:       h.spans[1].Range,
				Epoch:       firstEpoch,
				Generation:  h.placed,
				Replicas:    []routes.Endpoint{{Node: nodeA.ID, Address: nodeA.Address, Role: shard.RolePrimary}},
			}
			assert.Equal(t, got, want, "the route must name the placement and the address of its node")
		})

		t.Run("returns ErrNoRoute before the router has read the namespace", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			_, err := h.machine.Route(t.Context(), h.keys[1])
			assert.ErrorIs(t, err, routes.ErrNoRoute, "a router that has not read the namespace must not have a route")
		})

		t.Run("returns ErrNoRoute for a shard without a placement", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.place = false })
			h.commit(t, cellScope, &store.Mutation{Put: []shard.Placement{h.placement(1, nodeA.ID, firstEpoch)}})
			h.start(t)
			_, err := h.machine.Route(t.Context(), h.keys[2])
			assert.ErrorIs(t, err, routes.ErrNoRoute, "a shard without a placement must not have a route")
		})

		t.Run("returns ErrNoRoute for a shard whose node the view does not list", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.step(t, machine.Started{Reply: h.call()})
			h.run(t)
			h.follow(t)
			h.view(t, cellScope, nodeB)
			_, err := h.machine.Route(t.Context(), h.keys[1])
			assert.ErrorIs(t, err, routes.ErrNoRoute, "a shard whose node has left the view must not have a route")
		})

		t.Run("returns ErrClosed after the router has stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.step(t, machine.Stopped{})
			_, err := h.machine.Route(t.Context(), h.keys[1])
			assert.ErrorIs(t, err, routes.ErrClosed, "a stopped router must refuse every key")
		})

		t.Run("returns ErrNoRoute for a key of a position outside the cell map", func(t *testing.T) {
			t.Parallel()
			h := halved(t)
			_, err := h.machine.Route(t.Context(), h.keys[0])
			assert.ErrorIs(t, err, routes.ErrNoRoute, "a position outside the cell map must not have a route")
		})

		t.Run("returns the route of a key of each cell of a cell map with two cells", func(t *testing.T) {
			t.Parallel()
			h := halved(t)
			h.commit(t, rootScope, &store.Mutation{Put: []shard.Placement{{
				Shard: shard.ID{ID128: cellKey},
				Range: shard.Range{End: middle},
			}}})
			h.commit(t, cellScope, &store.Mutation{Put: []shard.Placement{
				h.placement(0, nodeA.ID, firstEpoch),
				h.placement(1, nodeA.ID, firstEpoch),
			}})
			h.follow(t)
			h.view(t, cellScope, nodeA)
			first, err := h.machine.Route(t.Context(), h.keys[1])
			assert.NoError(t, err, "a key of the first cell must have a route")
			last, err := h.machine.Route(t.Context(), h.keys[3])
			assert.NoError(t, err, "a key of the second cell must have a route")
			expect.Equal(t, first.Cell, cellID, "the route of a key of the first cell must name the first cell")
			expect.Equal(
				t,
				last.Cell,
				otherScope.Cell,
				"the route of a key of the second cell must name the second cell",
			)
		})

		t.Run("returns the route that a step published while the router steps", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "Route returns the route that a step of the router published for the slot", race)
		})
	})
}

// halved returns a harness of a router that serves a namespace whose root lists one cell,
// the second cell of the tests, from slot 2 on. The controller of the cell has placed slots 2
// and 3 on node A, and the view of the cell lists node A.
func halved(tb testCase) *harness {
	tb.Helper()
	h := newHarness(tb, func(s *setup) {
		s.root.Put = []shard.Placement{{Shard: shard.ID{ID128: otherKey}, Range: shard.Range{Start: middle}}}
		s.place = false
	})
	h.commit(tb, otherScope, &store.Mutation{Put: []shard.Placement{
		h.placement(2, nodeA.ID, firstEpoch),
		h.placement(3, nodeA.ID, firstEpoch),
	}})
	to := h.call()
	h.step(tb, machine.Started{Reply: to})
	h.run(tb)
	h.follow(tb)
	h.view(tb, otherScope, nodeA)
	assert.Nil(tb, h.replied(tb, to).Err, "the router must serve the namespace of one cell")
	return h
}

// race draws a script from c. One client starts the router and plays the script, while the
// readers call Route for the key of each slot. Each step of the router is a call of the
// history for each slot. The output of the call is what Route returns for the key of the
// slot after the step. The history must be linearizable against routeSpec, one slot at a
// time. Over 1,000 cases with GOMAXPROCS at 4, the readers called Route during 52% of the
// steps.
func race(c *prop.Case) {
	h := newHarness(c)
	entry := prop.Integer(0, scriptKinds*slots-1)
	script := c.Draw(prop.List(entry, prop.MinSize(1), prop.MaxSize(scriptSize)), "script")
	calls := c.History()
	h.wrap = func(step func()) {
		open := make([]history.Call, 0, slots)
		for i := range slots {
			open = append(open, calls.Invoke(stepClient+i, stepOperation, nil, i))
		}
		step()
		for i, key := range h.keys {
			r, err := h.machine.Route(c.Context(), key)
			open[i].OK(routeState{route: r, found: err == nil})
		}
	}
	done := make(chan struct{})
	outcomes := history.Concurrently(readers+1, within, func(client int) (any, error) {
		if client == 0 {
			defer close(done)
			h.start(c)
			play(c, h, script)
			return nil, nil
		}
		for range reads {
			select {
			case <-done:
				return nil, nil
			default:
			}
			for i, key := range h.keys {
				call := calls.Invoke(client, routeOperation, nil, i)
				r, err := h.machine.Route(c.Context(), key)
				call.OK(routeState{route: r, found: err == nil})
			}
		}
		return nil, nil
	})
	for _, o := range outcomes {
		assert.True(c, o.Finished, "every client of the property must finish")
	}
	history.Linearizable(
		c,
		calls,
		routeSpec,
		"Route must return the route that a step of the router published for the slot",
	)
}

// play plays script on the router of h, from the state in which the router serves. Each
// entry is a kind of step for one slot: the kind is the entry divided by the number of slots,
// and the slot is the remainder. After each entry, the harness passes everything that the
// store and the membership have to the router.
func play(c *prop.Case, h *harness, script []int) {
	var epochs [slots]epoch.Epoch
	recreated := byte(0)
	for _, entry := range script {
		i, kind := entry%slots, entry/slots
		switch kind {
		case scriptView:
			h.view(c, h.cell, scriptViews[i]...)
		case scriptRecreate:
			recreated++
			inc, key := recreation(recreated)
			h.recreate(c, inc, key)
			h.follow(c)
			h.view(c, h.cell, nodeA, nodeB)
			epochs = [slots]epoch.Epoch{}
		default:
			epochs[i]++
			p := h.placement(i, scriptWriters[kind], firstEpoch+epochs[i])
			h.commit(c, h.cell, &store.Mutation{Put: []shard.Placement{p}})
			h.follow(c)
		}
	}
}
