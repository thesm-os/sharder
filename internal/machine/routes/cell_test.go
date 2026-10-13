// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package routes_test

import (
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/internal/follow"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/routes"
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

// The positions of the placements of the tests that start at no slot of the cell.
var (
	// beyond is the position of slot 4, the first slot after the four slots of the namespace.
	beyond = []byte{0, 0, 0, 4}
	// between is a position of three bytes, which is not the position of a slot.
	between = []byte{0, 0, 1}
)

// movedAddress is the address of node A after it has moved.
const movedAddress = "node-a:7001"

// durations returns the Duration of each Timer of timers.
func durations(timers []machine.Timer) []time.Duration {
	ds := make([]time.Duration, 0, len(timers))
	for _, tm := range timers {
		ds = append(ds, tm.Duration)
	}
	return ds
}
