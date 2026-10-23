// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package routes

import (
	"context"

	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/telemetry"

	"go.thesmos.sh/sharder/shard"
)

// The names of the instruments that a router reports.
const (
	// lookupsName is the counter of the lookups of routes, per result.
	lookupsName telemetry.InstrumentName = "sharder.router.lookups"
	// routesName is the gauge of the routes in the cache of each cell.
	routesName telemetry.InstrumentName = "sharder.router.cache.routes"
	// lagName is the gauge of the generations of each cell that a Refresh waits for and the
	// router has not applied.
	lagName telemetry.InstrumentName = "sharder.router.refresh.lag"
)

// The keys of the attributes of the instruments and the logs.
const (
	// attrNamespace is the namespace of the router.
	attrNamespace = "namespace"
	// attrCell is the cell of an instrument.
	attrCell = "cell"
	// attrResult is the result of a lookup.
	attrResult = "result"
	// attrError is an error that does not stop the router.
	attrError = "error"
)

// The results of a lookup.
const (
	// resultHit is a call of Route that returns a route.
	resultHit = "hit"
	// resultMiss is a call of Route that returns ErrNoRoute.
	resultMiss = "miss"
	// resultRedirect is a Refresh, which a client sends after a server has redirected it.
	resultRedirect = "redirect"
)

// The units of the instruments, in the notation of UCUM.
const (
	unitLookup     = "{lookup}"
	unitRoute      = "{route}"
	unitGeneration = "{generation}"
)

// metrics are the instruments of a router that are bound to its namespace alone.
type metrics struct {
	// hits counts the calls of Route that return a route.
	hits telemetry.Counter
	// misses counts the calls of Route that return ErrNoRoute.
	misses telemetry.Counter
	// redirects counts the Refresh calls.
	redirects telemetry.Counter
}

// newMetrics returns the instruments of a router from r, bound to the namespace ns and to
// each result of a lookup.
func newMetrics(r telemetry.Reporter, ns shard.Namespace) *metrics {
	namespace := telemetry.AttrString(attrNamespace, string(ns))
	lookups := r.Counter(telemetry.InstrumentSpec{
		Name: lookupsName, Unit: unitLookup,
		Description: "Route lookups, per result: hit, miss, redirect.",
	})
	return &metrics{
		hits:      lookups.With([]telemetry.Attr{namespace, telemetry.AttrString(attrResult, resultHit)}),
		misses:    lookups.With([]telemetry.Attr{namespace, telemetry.AttrString(attrResult, resultMiss)}),
		redirects: lookups.With([]telemetry.Attr{namespace, telemetry.AttrString(attrResult, resultRedirect)}),
	}
}

// release ends every instrument of m.
func (m *metrics) release() {
	m.hits.Release()
	m.misses.Release()
	m.redirects.Release()
}

// cellMetrics are the instruments of one cell, each bound to the namespace and to the cell.
type cellMetrics struct {
	// routes is the number of routes in the cache of the cell.
	routes telemetry.Gauge
	// lag is the highest generation that a Refresh of the cell waits for, minus the
	// generation that the router has applied, and at least zero.
	lag telemetry.Gauge
}

// newCellMetrics returns the instruments of the cell id of the namespace ns from r.
func newCellMetrics(r telemetry.Reporter, ns shard.Namespace, id shard.CellID) cellMetrics {
	attrs := []telemetry.Attr{
		telemetry.AttrString(attrNamespace, string(ns)),
		telemetry.AttrString(attrCell, string(id)),
	}
	return cellMetrics{
		routes: r.Gauge(telemetry.InstrumentSpec{
			Name: routesName, Unit: unitRoute, Aggregation: telemetry.GaugeAggregationSum,
			Description: "Routes in a router's cache, per cell.",
		}).With(attrs),
		lag: r.Gauge(telemetry.InstrumentSpec{
			Name: lagName, Unit: unitGeneration, Aggregation: telemetry.GaugeAggregationMax,
			Description: "Generations of a cell that a Refresh waits for and the router has not applied.",
		}).With(attrs),
	}
}

// report sets the gauges of the cell to the count of its routes and to the lag of the
// generation applied behind the generation highest.
func (m cellMetrics) report(routes int, highest, applied epoch.Epoch) {
	m.routes.Set(context.Background(), float64(routes))
	m.lag.Set(context.Background(), float64(max(highest, applied)-applied))
}

// release ends every instrument of m.
func (m cellMetrics) release() {
	m.routes.Release()
	m.lag.Release()
}
