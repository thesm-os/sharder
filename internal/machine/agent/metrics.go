// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent

import (
	"go.thesmos.sh/core/telemetry"

	"go.thesmos.sh/sharder/shard"
)

// The names of the instruments that an agent reports.
const (
	// refusalsName is the counter of the fence acquisitions that returned epoch.ErrFenced.
	refusalsName telemetry.InstrumentName = "sharder.fence.refusals"
	// lagName is the gauge of the generations of the scope of the cell that the controller
	// has committed and the agent has not applied.
	lagName telemetry.InstrumentName = "sharder.generation.lag"
)

// The keys of the attributes of the instruments and the logs.
const (
	// attrNamespace is the namespace of the agent.
	attrNamespace = "namespace"
	// attrNode is the node of the agent.
	attrNode = "node"
	// attrShard is the shard of a replica.
	attrShard = "shard"
	// attrError is an error that does not stop the agent.
	attrError = "error"
)

// The units of the instruments, in the notation of UCUM.
const (
	unitAcquisition = "{acquisition}"
	unitGeneration  = "{generation}"
)

// metrics are the instruments of an agent, each bound to the namespace and the node of the
// agent.
type metrics struct {
	// refusals counts the fence acquisitions that returned epoch.ErrFenced.
	refusals telemetry.Counter
	// lag is the number of generations that the controller has committed and the agent has
	// not applied, at the last report that the controller accepted.
	lag telemetry.Gauge
}

// newMetrics returns the instruments of an agent from r, bound to the namespace ns and to
// the node.
func newMetrics(r telemetry.Reporter, ns shard.Namespace, node shard.NodeID) *metrics {
	namespace, of := telemetry.AttrString(attrNamespace, string(ns)), telemetry.AttrString(attrNode, string(node))
	return &metrics{
		refusals: r.Counter(telemetry.InstrumentSpec{
			Name: refusalsName, Unit: unitAcquisition,
			Description: "Fence acquisitions that returned epoch.ErrFenced.",
		}).With([]telemetry.Attr{namespace, of}),
		lag: r.Gauge(telemetry.InstrumentSpec{
			Name: lagName, Unit: unitGeneration, Aggregation: telemetry.GaugeAggregationMax,
			Description: "Generations between the store and each agent.",
		}).With([]telemetry.Attr{namespace, of}),
	}
}

// release ends every instrument of m.
func (m *metrics) release() {
	m.refusals.Release()
	m.lag.Release()
}
