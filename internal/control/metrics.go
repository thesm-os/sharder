// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package control

import (
	"context"

	"go.thesmos.sh/core/telemetry"

	"go.thesmos.sh/sharder/shard"
)

// The names of the instruments that a controller reports.
const (
	// transitionsName is the gauge of the transitions in flight, per protocol and phase.
	transitionsName telemetry.InstrumentName = "sharder.transitions.in_flight"
	// unplacedName is the gauge of the shards of the scope without an assigned writer.
	unplacedName telemetry.InstrumentName = "sharder.shards.unplaced"
	// passName is the histogram of the duration of each planning pass, per mode.
	passName telemetry.InstrumentName = "sharder.planner.pass.duration"
	// movesName is the counter of the moves that the controller starts, per reason.
	movesName telemetry.InstrumentName = "sharder.moves"
	// deferredName is the counter of the disruptions and the moves that the budget
	// deferred.
	deferredName telemetry.InstrumentName = "sharder.budget.deferred"
	// reservationsName is the gauge of the reservations that have not ended, per state.
	reservationsName telemetry.InstrumentName = "sharder.budget.reservations"
	// unknownName is the counter of the attempts of commits that failed with an unknown
	// outcome.
	unknownName telemetry.InstrumentName = "sharder.commits.unknown"
	// gapName is the histogram of the writer gaps, per reason.
	gapName telemetry.InstrumentName = "sharder.writer.gap"
)

// The keys of the attributes of the instruments and the logs.
const (
	// attrNamespace is the namespace of the scope.
	attrNamespace = "namespace"
	// attrCell is the cell of the scope, and empty for the root scope.
	attrCell = "cell"
	// attrProtocol is the protocol of a transition.
	attrProtocol = "protocol"
	// attrPhase is the phase of a transition.
	attrPhase = "phase"
	// attrMode is the mode of a planning pass.
	attrMode = "mode"
	// attrReason is the reason of a move or of a writer gap.
	attrReason = "reason"
	// attrState is the state of a reservation.
	attrState = "state"
)

// The values of the attributes of the instruments.
const (
	// modeEmergency is the mode of a pass that restores missing or faulted replicas first.
	modeEmergency = "emergency"
	// modePeriodic is the mode of every other pass.
	modePeriodic = "periodic"
	// reasonFailure is the reason of a move of a replica that is missing, faulted or on a
	// node that has left, and of a writer gap that the end of a node starts.
	reasonFailure = "failure"
	// reasonDrain is the reason of a move of a replica off a draining node.
	reasonDrain = "drain"
	// reasonRebalance is the reason of every other move.
	reasonRebalance = "rebalance"
	// reasonPlanned is the reason of a writer gap that a commit starts.
	reasonPlanned = "planned"
	// statePending is the state of a reservation that the budget has not admitted.
	statePending = "pending"
	// stateAdmitted is the state of a reservation that the budget has admitted.
	stateAdmitted = "admitted"
)

// The units of the instruments, in the notation of UCUM.
const (
	unitTransition  = "{transition}"
	unitShard       = "{shard}"
	unitSeconds     = "s"
	unitMove        = "{move}"
	unitDisruption  = "{disruption}"
	unitReservation = "{reservation}"
	unitCommit      = "{commit}"
)

// inFlight is a protocol and a phase, the key of the count of the transitions in flight.
type inFlight struct {
	// protocol is the protocol of the transitions.
	protocol shard.StrategyName
	// phase is the phase of the transitions.
	phase shard.Phase
}

// metrics are the instruments of a controller, each bound to the namespace and the cell of
// its scope.
type metrics struct {
	// transitions is the gauge of the transitions in flight, which gauge binds to each
	// protocol and phase that it counts.
	transitions telemetry.Gauge
	// inflight maps each protocol and phase that the controller has counted to its bound
	// gauge.
	inflight map[inFlight]telemetry.Gauge
	// counts maps each protocol and phase to the transitions in flight. gauge reuses it.
	counts map[inFlight]int
	// unplaced counts the shards without an assigned writer.
	unplaced telemetry.Gauge
	// emergency and periodic record the duration of the passes of each mode.
	emergency, periodic telemetry.Histogram
	// failure, drain and rebalance count the moves of each reason.
	failure, drain, rebalance telemetry.Counter
	// deferred counts the deferrals of the budget.
	deferred telemetry.Counter
	// pending and admitted count the reservations of each state.
	pending, admitted telemetry.Gauge
	// unknown counts the attempts of commits with an unknown outcome. The scope adds to it.
	unknown telemetry.Counter
	// plannedGap and failureGap record the writer gaps of each reason.
	plannedGap, failureGap telemetry.Histogram
	// attrs are the namespace and the cell.
	attrs [2]telemetry.Attr
}

// newMetrics returns the instruments of a controller of the scope s from r.
func newMetrics(r telemetry.Reporter, s shard.Scope) *metrics {
	ns, cell := telemetry.AttrString(attrNamespace, string(s.Namespace)), telemetry.AttrString(attrCell, string(s.Cell))
	pass := r.Histogram(telemetry.InstrumentSpec{
		Name: passName, Unit: unitSeconds, Description: "Duration of each planning pass, per mode.",
	})
	moves := r.Counter(telemetry.InstrumentSpec{
		Name: movesName, Unit: unitMove, Description: "Replicas moved, per reason.",
	})
	reservations := r.Gauge(telemetry.InstrumentSpec{
		Name: reservationsName, Unit: unitReservation, Aggregation: telemetry.GaugeAggregationSum,
		Description: "Reservations that have not ended, per state.",
	})
	gaps := r.Histogram(telemetry.InstrumentSpec{
		Name: gapName, Unit: unitSeconds,
		Description: "Time from the stop of a writer to the first serving report of the next writer, per reason.",
	})
	return &metrics{
		transitions: r.Gauge(telemetry.InstrumentSpec{
			Name: transitionsName, Unit: unitTransition, Aggregation: telemetry.GaugeAggregationSum,
			Description: "Transitions started and not finished, per protocol and phase.",
		}),
		inflight: make(map[inFlight]telemetry.Gauge),
		counts:   make(map[inFlight]int),
		unplaced: r.Gauge(telemetry.InstrumentSpec{
			Name: unplacedName, Unit: unitShard, Aggregation: telemetry.GaugeAggregationSum,
			Description: "Shards without an assigned writer.",
		}).With([]telemetry.Attr{ns, cell}),
		emergency: pass.With([]telemetry.Attr{ns, cell, telemetry.AttrString(attrMode, modeEmergency)}),
		periodic:  pass.With([]telemetry.Attr{ns, cell, telemetry.AttrString(attrMode, modePeriodic)}),
		failure:   moves.With([]telemetry.Attr{ns, cell, telemetry.AttrString(attrReason, reasonFailure)}),
		drain:     moves.With([]telemetry.Attr{ns, cell, telemetry.AttrString(attrReason, reasonDrain)}),
		rebalance: moves.With([]telemetry.Attr{ns, cell, telemetry.AttrString(attrReason, reasonRebalance)}),
		deferred: r.Counter(telemetry.InstrumentSpec{
			Name: deferredName, Unit: unitDisruption, Description: "Disruptions and moves that the budget deferred.",
		}).With([]telemetry.Attr{ns, cell}),
		pending:  reservations.With([]telemetry.Attr{ns, cell, telemetry.AttrString(attrState, statePending)}),
		admitted: reservations.With([]telemetry.Attr{ns, cell, telemetry.AttrString(attrState, stateAdmitted)}),
		unknown: r.Counter(telemetry.InstrumentSpec{
			Name: unknownName, Unit: unitCommit, Description: "Commits that failed with an unknown outcome.",
		}).With([]telemetry.Attr{ns, cell}),
		plannedGap: gaps.With([]telemetry.Attr{ns, cell, telemetry.AttrString(attrReason, reasonPlanned)}),
		failureGap: gaps.With([]telemetry.Attr{ns, cell, telemetry.AttrString(attrReason, reasonFailure)}),
		attrs:      [2]telemetry.Attr{ns, cell},
	}
}

// release ends every instrument of m.
func (m *metrics) release() {
	for _, g := range m.inflight {
		g.Release()
	}
	for _, g := range [...]telemetry.Gauge{m.unplaced, m.pending, m.admitted} {
		g.Release()
	}
	for _, h := range [...]telemetry.Histogram{m.emergency, m.periodic, m.plannedGap, m.failureGap} {
		h.Release()
	}
	for _, c := range [...]telemetry.Counter{m.failure, m.drain, m.rebalance, m.deferred, m.unknown} {
		c.Release()
	}
}

// gauge sets the gauges of the transitions, the unplaced shards and the reservations to the
// committed state of the scope. It binds the gauge of the transitions to each protocol and
// phase on its first count, and sets a protocol and phase without transitions to zero.
func (c *Controller) gauge() {
	ctx := context.Background()
	mt := c.metrics
	clear(mt.counts)
	for _, t := range c.assignment.transitions {
		k := inFlight{protocol: t.Protocol, phase: t.Phase}
		mt.counts[k]++
		if _, ok := mt.inflight[k]; !ok {
			mt.inflight[k] = mt.transitions.With([]telemetry.Attr{
				mt.attrs[0], mt.attrs[1],
				telemetry.AttrString(attrProtocol, string(k.protocol)),
				telemetry.AttrString(attrPhase, k.phase.String()),
			})
		}
	}
	for k, g := range mt.inflight {
		g.Set(ctx, float64(mt.counts[k]))
	}
	mt.unplaced.Set(ctx, float64(c.assignment.unplaced))
	pending, admitted := 0, 0
	for _, e := range c.budget.reservations.All() {
		if e.r.Admitted.IsZero() {
			pending++
			continue
		}
		admitted++
	}
	mt.pending.Set(ctx, float64(pending))
	mt.admitted.Set(ctx, float64(admitted))
}
