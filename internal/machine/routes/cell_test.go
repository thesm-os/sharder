// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package routes_test

import (
	"encoding/binary"
	"encoding/json"
	"slices"
	"strconv"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/internal/follow"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/routes"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// TestCell checks the reads of the scope and of the views of a cell, and the routes of its
// slots.
func TestCell(t *testing.T) {
	t.Parallel()

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("replaces the route of a placement that a generation puts", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			gen := h.commit(
				t,
				cellScope,
				&store.Mutation{Put: []shard.Placement{h.placement(1, nodeB.ID, firstEpoch+1)}},
			)
			h.follow(t)
			got, err := h.machine.Route(t.Context(), h.keys[1])
			assert.NoError(t, err, "the moved shard must have a route")
			expect.Equal(t, got.Epoch, firstEpoch+1, "the route must have the epoch of the new placement")
			expect.Equal(t, got.Generation, gen, "the route must have the generation of the commit of the placement")
			expect.Equal(
				t,
				got.Replicas,
				[]routes.Endpoint{{Node: nodeB.ID, Address: nodeB.Address, Role: shard.RolePrimary}},
				"the route must name the node of the new placement",
			)
		})

		t.Run("keeps the route of a slot that a generation does not put", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.commit(t, cellScope, &store.Mutation{Put: []shard.Placement{h.placement(1, nodeB.ID, firstEpoch+1)}})
			h.follow(t)
			got, err := h.machine.Route(t.Context(), h.keys[2])
			assert.NoError(t, err, "a shard that did not move must keep its route")
			assert.Equal(t, got.Generation, h.placed, "the route must keep the generation of its placement")
		})

		t.Run("replaces the routes of the cell with a snapshot after a compaction", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			gen := h.commit(
				t,
				cellScope,
				&store.Mutation{Put: []shard.Placement{h.placement(1, nodeB.ID, firstEpoch+1)}},
			)
			h.watchFails(t, cellScope, store.ErrCompacted)
			h.elapse(t, follow.RetryBase/2)
			kept, err := h.machine.Route(t.Context(), h.keys[2])
			assert.NoError(t, err, "a shard of the snapshot must have a route")
			expect.Equal(t, kept.Generation, gen, "the route must have the generation of the snapshot")
			moved, err := h.machine.Route(t.Context(), h.keys[1])
			assert.NoError(t, err, "the moved shard must have a route")
			expect.Equal(
				t,
				moved.Replicas,
				[]routes.Endpoint{{Node: nodeB.ID, Address: nodeB.Address, Role: shard.RolePrimary}},
				"the snapshot must give the moved shard the node of its new placement",
			)
		})

		t.Run("gives a route the address of its node in the latest view", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			moved := nodeA
			moved.Address = movedAddress
			h.view(t, cellScope, moved, nodeB)
			got, err := h.machine.Route(t.Context(), h.keys[1])
			assert.NoError(t, err, "the shard must keep its route")
			assert.Equal(
				t,
				got.Replicas,
				[]routes.Endpoint{{Node: nodeA.ID, Address: movedAddress, Role: shard.RolePrimary}},
				"the route must have the address of the node in the latest view",
			)
		})

		t.Run("keeps the route of a slot whose node a view does not change", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			before, err := h.machine.Route(t.Context(), h.keys[1])
			assert.NoError(t, err, "the shard must have a route before the view")
			h.view(t, cellScope, nodeA, nodeB, nodeC)
			after, err := h.machine.Route(t.Context(), h.keys[1])
			assert.NoError(t, err, "the shard must keep its route")
			assert.Equal(t, after.Replicas, before.Replicas,
				"a view that adds a node without a replica must keep the published route", assert.ByIdentity())
		})

		t.Run("keeps the route of a slot whose node a repeated view lists again", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			before, err := h.machine.Route(t.Context(), h.keys[1])
			assert.NoError(t, err, "the shard must have a route before the view")
			h.view(t, cellScope, nodeA, nodeB)
			after, err := h.machine.Route(t.Context(), h.keys[1])
			assert.NoError(t, err, "the shard must keep its route")
			assert.Equal(t, after.Replicas, before.Replicas,
				"a view with the same nodes must keep the published route", assert.ByIdentity())
		})

		t.Run("keeps the route of a slot on another node than the node that a view moves", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.commit(t, cellScope, &store.Mutation{Put: []shard.Placement{h.placement(1, nodeB.ID, firstEpoch+1)}})
			h.follow(t)
			before, err := h.machine.Route(t.Context(), h.keys[1])
			assert.NoError(t, err, "the shard on node B must have a route before the view")
			moved := nodeA
			moved.Address = movedAddress
			h.view(t, cellScope, moved, nodeB)
			after, err := h.machine.Route(t.Context(), h.keys[1])
			assert.NoError(t, err, "the shard on node B must keep its route")
			assert.Equal(t, after.Replicas, before.Replicas,
				"a view that moves node A must keep the route of a shard on node B", assert.ByIdentity())
		})

		t.Run("removes the route of a slot whose node without an address leaves the view", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			unaddressed := nodeA
			unaddressed.Address = ""
			h.view(t, cellScope, unaddressed, nodeB)
			_, err := h.machine.Route(t.Context(), h.keys[1])
			assert.NoError(t, err, "a shard on a node without an address must have a route")
			h.view(t, cellScope, nodeB)
			_, err = h.machine.Route(t.Context(), h.keys[1])
			assert.ErrorIs(t, err, routes.ErrNoRoute, "a shard whose node has left the view must not have a route")
		})

		t.Run("returns the route of a shard again once its node is back in the view", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.view(t, cellScope, nodeB)
			_, err := h.machine.Route(t.Context(), h.keys[1])
			assert.ErrorIs(t, err, routes.ErrNoRoute, "a shard whose node has left the view must not have a route")
			h.view(t, cellScope, nodeA, nodeB)
			got, err := h.machine.Route(t.Context(), h.keys[1])
			assert.NoError(t, err, "a shard whose node is back in the view must have a route")
			assert.Equal(t, got.Generation, h.placed, "the route must keep the generation of its placement")
		})

		t.Run("stops at a placement that starts beyond the slots of its cell", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			p := h.placement(1, nodeA.ID, firstEpoch+1)
			p.Range = shard.Range{Start: beyond}
			h.commit(t, cellScope, &store.Mutation{Put: []shard.Placement{p}})
			h.follow(t)
			assert.ErrorIs(t, h.machine.Err(), routes.ErrMapping, "a placement outside the slots must stop the router")
		})

		t.Run("stops at a placement that starts between two slots of its cell", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			p := h.placement(1, nodeA.ID, firstEpoch+1)
			p.Range = shard.Range{Start: between}
			h.commit(t, cellScope, &store.Mutation{Put: []shard.Placement{p}})
			h.follow(t)
			assert.ErrorIs(t, h.machine.Err(), routes.ErrMapping, "a placement between two slots must stop the router")
		})

		t.Run("returns ErrMapping to Started for a snapshot with a placement outside the slots", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			p := h.placement(1, nodeA.ID, firstEpoch+1)
			p.Range = shard.Range{Start: between}
			h.commit(t, cellScope, &store.Mutation{Put: []shard.Placement{p}})
			to := h.start(t)
			assert.ErrorIs(t, h.replied(t, to).Err, routes.ErrMapping,
				"a snapshot with a placement outside the slots must fail Started")
		})

		t.Run("watches the views of a cell again after a transient error", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.viewFails(t, cellScope, errBusy)
			assert.Empty(t, h.viewers, "the failed watch must have ended")
			h.elapse(t, routes.RetryBase/2)
			assert.Length(t, h.viewers, 1, "the router must watch the views again after the backoff")
		})

		t.Run("doubles the backoff of the views with each error in a row", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.viewFails(t, cellScope, errBusy)
			h.elapse(t, routes.RetryBase/2)
			h.viewFails(t, cellScope, errBusy)
			assert.Contains(t, durations(h.timers), routes.RetryBase, "the second backoff must double the ceiling")
		})

		t.Run("starts the backoff of the views again after a view", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.viewFails(t, cellScope, errBusy)
			h.elapse(t, routes.RetryBase/2)
			h.view(t, cellScope, nodeA)
			h.viewFails(t, cellScope, errBusy)
			assert.Equal(t, durations(h.timers), []time.Duration{routes.RetryBase / 2},
				"a view must start the backoff again")
		})

		t.Run("stops at an error of the views that is not transient", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.viewFails(t, cellScope, errBroken)
			assert.ErrorIs(t, h.machine.Err(), errBroken, "Err must return the error of the views")
		})

		t.Run("returns out unchanged for a view of a watch that it does not run", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Viewed{Token: 99}, out)
			assert.Equal(t, got, out, "the router must ignore a view of a watch that it does not run")
		})

		t.Run("returns out unchanged for a Timer that it did not arm", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Elapsed{Token: 99}, out)
			assert.Equal(t, got, out, "the router must ignore a Timer that it did not arm")
		})

		t.Run("keeps the generation of a cell over a retry of its follower", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.watchFails(t, cellScope, errBusy)
			h.elapse(t, follow.RetryBase/2)
			to := h.refresh(t, incA, cellID, h.placed)
			assert.Nil(t, h.replied(t, to).Err, "the router must keep the generation that it applied")
		})
	})
}

// TestCellAllocs checks the allocations of a view of a cell of four slots. MaxAllocs counts
// the allocations of the whole process, so the test does not run in parallel.
func TestCellAllocs(t *testing.T) {
	t.Run("Step", func(t *testing.T) {
		tests := []struct {
			name     string
			affected int
			want     uint64
		}{
			{name: "of a view that moves no node with a replica", affected: 0, want: viewAllocs},
			{name: "of a view that moves the node of every slot", affected: slots, want: viewAllocs + 2*slots},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				h := sized(t, slots, tt.affected)
				views := moves(h, tt.affected)
				out := make([]machine.Effect, 0, 1)
				i := 0
				expect.MaxAllocs(t, func() {
					out = h.machine.Step(views[i%2], out[:0])
					i++
				}, tt.want, "a view must allocate its addresses and the route of each slot that it moves")
				expect.Empty(t, out, "the measurement must take views that the router applies")
			})
		}
	})
}

// BenchmarkCell measures the step of a view over cells of 1,000 and 100,000 slots. The view
// moves no node with a replica, the node of one slot, or the node of every slot.
func BenchmarkCell(b *testing.B) {
	b.Run("Step", func(b *testing.B) {
		for _, total := range []int{1_000, 100_000} {
			b.Run(strconv.Itoa(total), func(b *testing.B) {
				for _, affected := range []int{0, 1, total} {
					b.Run(strconv.Itoa(affected), func(b *testing.B) {
						h := sized(b, total, affected)
						views := moves(h, affected)
						out := make([]machine.Effect, 0, 1)
						i := 0

						c := bench.Start(b).MaxAllocs(viewAllocs + 2*uint64(affected))
						defer c.End()

						for c.Loop() {
							out = h.machine.Step(views[i%2], out[:0])
							i++
						}

						assert.Empty(b, out, "the benchmark must measure views that the router applies")
					})
				}
			})
		}
	})
}

// The positions of the placements of the tests that start at no slot of the cell.
var (
	// beyond is the position of slot 4, the first slot after the four slots of the namespace.
	beyond = []byte{0, 0, 0, 4}
	// between is a position of three bytes, which is not the position of a slot.
	between = []byte{0, 0, 1}
)

// movedAddress is the address of node A after it has moved.
const movedAddress = "node-a:7001"

// The measurements of a view.
const (
	// viewAllocs is the allocations of a view that moves no node with a replica.
	viewAllocs = 0
	// sizedPage is the number of placements on each page that the harness of a large cell
	// reads.
	sizedPage = 1000
	// sizedTag is the first byte of the ID of each shard of a large cell.
	sizedTag = 0x03
)

// durations returns the Duration of each Timer of timers.
func durations(timers []machine.Timer) []time.Duration {
	ds := make([]time.Duration, 0, len(timers))
	for _, tm := range timers {
		ds = append(ds, tm.Duration)
	}
	return ds
}

// sized returns a harness of a router that serves a namespace of one cell of total slots.
// The controller of the cell has placed the first affected slots on node A and the other
// slots on node B, and the view of the cell lists the nodes A, B and C. The router reports
// to a reporter that keeps no value, so a measurement counts the allocations of the router
// alone.
func sized(tb testCase, total, affected int) *harness {
	tb.Helper()
	h := newHarness(tb, func(s *setup) {
		s.spec.Partitioning.Config = json.RawMessage(`{"slots":` + strconv.Itoa(total) + `}`)
		s.place = false
		s.cfg.Reporter = noop.New()
	})
	h.limit = sizedPage
	ps := make([]shard.Placement, total)
	for i := range ps {
		writer := nodeB.ID
		if i < affected {
			writer = nodeA.ID
		}
		var sid id.ID128
		sid[0] = sizedTag
		binary.BigEndian.PutUint32(sid[12:], uint32(i))
		ps[i] = shard.Placement{
			Shard:    shard.ID{ID128: sid},
			Range:    h.spans[i].Range,
			Epoch:    firstEpoch,
			Reserved: firstEpoch,
			Sequence: 1,
			Replicas: []shard.Replica{{Node: writer, Role: shard.RolePrimary}},
		}
	}
	h.placed = h.commit(tb, cellScope, &store.Mutation{Put: ps})
	h.start(tb)
	h.view(tb, cellScope, nodeA, nodeB, nodeC)
	return h
}

// moves returns two views of the cell of h for the watch of the views of the router of h.
// The first view gives node A another address when affected is above zero, and node C
// otherwise. The second view lists the nodes as sized does. A step of each view after the
// other moves the node of affected slots.
func moves(h *harness, affected int) [2]machine.Viewed {
	moved := nodeC
	if affected > 0 {
		moved = nodeA
	}
	moved.Address = movedAddress
	nodes := []shard.Node{nodeA, nodeB, nodeC}
	away := slices.Clone(nodes)
	away[slices.IndexFunc(away, func(n shard.Node) bool { return n.ID == moved.ID })] = moved
	w := h.viewers[0]
	return [2]machine.Viewed{
		{
			View:  membership.View{Scope: w.Scope, Nodes: away, Revision: h.revision + 1, Incarnation: w.Incarnation},
			Token: w.Token,
		},
		{
			View:  membership.View{Scope: w.Scope, Nodes: nodes, Revision: h.revision + 2, Incarnation: w.Incarnation},
			Token: w.Token,
		},
	}
}
