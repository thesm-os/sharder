// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package routes_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/coretest/telemetrytest"
	"go.thesmos.sh/core/telemetry"

	"go.thesmos.sh/sharder/internal/follow"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/routes"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// The names of the instruments that the tests read.
const (
	lookupsName telemetry.InstrumentName = "sharder.router.lookups"
	routesName  telemetry.InstrumentName = "sharder.router.cache.routes"
	lagName     telemetry.InstrumentName = "sharder.generation.lag"
)

// The results of a lookup, which the counter of lookups has as an attribute.
const (
	resultHit      = "hit"
	resultMiss     = "miss"
	resultRedirect = "redirect"
)

// The keys of the attributes that a router binds to its instruments.
const (
	keyNamespace = "namespace"
	keyCell      = "cell"
	keyResult    = "result"
)

// The attribute sets of the series that the tests read.
var (
	// hits is the series of the counter of lookups that returned a route.
	hits = []telemetry.Attr{
		telemetry.AttrString(keyNamespace, string(namespace)),
		telemetry.AttrString(keyResult, resultHit),
	}
	// misses is the series of the counter of lookups that returned ErrNoRoute.
	misses = []telemetry.Attr{
		telemetry.AttrString(keyNamespace, string(namespace)),
		telemetry.AttrString(keyResult, resultMiss),
	}
	// redirects is the series of the counter of lookups that a Refresh counts.
	redirects = []telemetry.Attr{
		telemetry.AttrString(keyNamespace, string(namespace)),
		telemetry.AttrString(keyResult, resultRedirect),
	}
	// ofCell is the series of each gauge of the cell of the tests.
	ofCell = []telemetry.Attr{
		telemetry.AttrString(keyNamespace, string(namespace)),
		telemetry.AttrString(keyCell, string(cellID)),
	}
)

// TestMetrics checks the instruments of a router.
func TestMetrics(t *testing.T) {
	t.Parallel()

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("counts each route that Route returns as a hit", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			for range 2 {
				_, err := h.machine.Route(t.Context(), h.keys[1])
				assert.NoError(t, err, "the shard must have a route")
			}
			assert.Equal(t, h.metrics.Values(lookupsName, hits...), []float64{1, 1}, "the router must count each hit")
		})

		t.Run("counts each ErrNoRoute of Route as a miss", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.view(t, cellScope, nodeB)
			_, err := h.machine.Route(t.Context(), h.keys[1])
			assert.ErrorIs(t, err, routes.ErrNoRoute, "the shard must not have a route")
			expect.Equal(t, h.metrics.Values(lookupsName, misses...), []float64{1}, "the router must count the miss")
			expect.Empty(t, h.metrics.Values(lookupsName, hits...), "the router must not count a miss as a hit")
		})

		t.Run("counts each Refresh as a redirect", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.refresh(t, incA, cellID, h.placed)
			assert.Equal(t, h.metrics.Values(lookupsName, redirects...), []float64{1},
				"the router must count the redirect")
		})

		t.Run("reports the routes of the cache of a cell", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			expect.Equal(t, latest(t, h.metrics, routesName, ofCell...), float64(slots),
				"the cache of the cell must have a route of each slot")
			h.commit(t, cellScope, &store.Mutation{Put: []shard.Placement{h.placement(1, noNode, firstEpoch+1)}})
			h.follow(t)
			expect.Equal(t, latest(t, h.metrics, routesName, ofCell...), float64(slots-1),
				"the cache of the cell must not count a slot without a route")
		})

		t.Run("reports the routes of the cache that a snapshot replaces", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.commit(t, cellScope, &store.Mutation{Put: []shard.Placement{h.placement(1, noNode, firstEpoch+1)}})
			h.watchFails(t, cellScope, store.ErrCompacted)
			h.elapse(t, follow.RetryBase/2)
			assert.Equal(t, latest(t, h.metrics, routesName, ofCell...), float64(slots-1),
				"the gauge must count the routes of the snapshot")
		})

		t.Run("reports the lag of a cell behind the generation that a Refresh waits for", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.refresh(t, incA, cellID, h.placed+3)
			expect.Equal(t, latest(t, h.metrics, lagName, ofCell...), 3.0,
				"the lag must be the generations that the router has not applied")
			h.commit(t, cellScope, &store.Mutation{Put: []shard.Placement{h.placement(1, nodeB.ID, firstEpoch+1)}})
			h.follow(t)
			expect.Equal(t, latest(t, h.metrics, lagName, ofCell...), 2.0,
				"the lag must fall with each generation that the router applies")
		})

		t.Run("reports a lag of zero for a Refresh of a generation that the router has applied", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.commit(t, cellScope, &store.Mutation{Put: []shard.Placement{h.placement(1, nodeB.ID, firstEpoch+1)}})
			h.follow(t)
			h.refresh(t, incA, cellID, h.placed)
			assert.Equal(t, latest(t, h.metrics, lagName, ofCell...), 0.0, "the lag must not fall below zero")
		})

		t.Run("releases its instruments at Stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.step(t, machine.Stopped{})
			for _, series := range [][]telemetry.Attr{hits, misses, redirects} {
				expect.Equal(
					t,
					h.metrics.Bound(lookupsName, series...),
					0,
					"the router must release the counter of lookups",
				)
			}
			expect.Equal(t, h.metrics.Bound(routesName, ofCell...), 0, "the router must release the gauge of routes")
			expect.Equal(t, h.metrics.Bound(lagName, ofCell...), 0, "the router must release the gauge of the lag")
		})

		t.Run("releases the instruments of the cells of an incarnation that it replaces", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			recreated := h.recreate(t, incB, recreatedKey)
			h.follow(t)
			h.view(t, recreated, nodeA)
			expect.Equal(t, h.metrics.Bound(routesName, ofCell...), 0, "the router must release the gauge of routes")
			ofRecreated := []telemetry.Attr{
				telemetry.AttrString(keyNamespace, string(namespace)),
				telemetry.AttrString(keyCell, string(recreated.Cell)),
			}
			expect.Equal(
				t,
				h.metrics.Bound(routesName, ofRecreated...),
				1,
				"the router must keep the gauge of the new cell",
			)
		})

		t.Run("releases the instruments of the cells of a read that it restarts", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.step(t, machine.Started{Reply: h.call()})
			h.run(t)
			h.follow(t)
			h.viewFails(t, cellScope, shard.ErrIncarnation)
			assert.Equal(t, h.metrics.Bound(routesName, ofCell...), 1,
				"the cell must have the gauge of routes of the new read alone")
		})
	})
}

// latest returns the last value of the series of the gauge name with the attribute set attrs
// in r, and stops tb when the gauge has no value in that series.
func latest(tb assert.TB, r *telemetrytest.Recorder, name telemetry.InstrumentName, attrs ...telemetry.Attr) float64 {
	tb.Helper()
	values := r.Values(name, attrs...)
	assert.NotEmpty(tb, values, "the gauge must have a value in the series")
	return values[len(values)-1]
}
