// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package config

import (
	"encoding/json"

	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/shard"
)

// Spec declares how one namespace is sharded. Resolve fills a field that the spec
// leaves empty from the preset of the spec, and then with the default of the field.
//
// Resolve refuses the zero Spec, because it does not have a namespace or a preset.
//
// # Encoding
//
// encoding/json encodes a Spec with the field names of its tags. The root stores the
// resolved spec of a namespace in that encoding.
//
// # Concurrency
//
// A copy of a Spec shares the slices of its fields with the original.
type Spec struct {
	// Namespace is the namespace of the spec.
	Namespace shard.Namespace `json:"namespace"`
	// Preset is the name of the registered preset of the spec. Resolve fills the
	// fields that the spec leaves empty from that preset.
	Preset PresetName `json:"preset"`

	// Partitioning selects the partitioner.
	Partitioning Selection `json:"partitioning,omitzero"`
	// Placement selects the placer.
	Placement Selection `json:"placement,omitzero"`
	// Replication selects the replica state model.
	Replication Selection `json:"replication,omitzero"`
	// Ownership selects the ownership mode.
	Ownership Selection `json:"ownership,omitzero"`
	// Handoff selects the handoff protocol.
	Handoff Selection `json:"handoff,omitzero"`
	// Topology selects where the controllers run.
	Topology Selection `json:"topology,omitzero"`
	// Routing selects where routers learn their routes.
	Routing Selection `json:"routing,omitzero"`

	// Load is the load model that placers balance.
	Load LoadModel `json:"load,omitzero"`
	// Budget bounds the unavailability and the work of planned changes.
	Budget Budget `json:"budget,omitzero"`
	// Cells bounds the size of each cell.
	Cells CellPolicy `json:"cells,omitzero"`
	// Version increases by one with every accepted change of the spec.
	Version epoch.Epoch `json:"version"`
}

// selection returns the field of s that selects the strategy of axis a, and nil for an
// axis without a selection in a spec.
func (s *Spec) selection(a Axis) *Selection {
	switch a {
	case Partitioning:
		return &s.Partitioning
	case Placement:
		return &s.Placement
	case Replication:
		return &s.Replication
	case Ownership:
		return &s.Ownership
	case Handoff:
		return &s.Handoff
	case Topology:
		return &s.Topology
	case Routing:
		return &s.Routing
	default:
		return nil
	}
}

// Selection selects a registered strategy by its name, and contains its configuration.
//
// A selection without a name selects the strategy of the preset. When a selection
// selects the strategy of the preset, Resolve merges its Config over the
// configuration of the preset as a JSON Merge Patch of RFC 7396. Resolve starts a
// selection of another strategy from the defaults of that strategy.
//
// # Concurrency
//
// A copy of a Selection shares the bytes of Config with the original.
type Selection struct {
	// Name is the registered name of the strategy, or empty for the strategy of the
	// preset.
	Name shard.StrategyName `json:"name,omitempty"`
	// Config is the JSON configuration of the strategy. The factory of the strategy
	// defines its schema.
	Config json.RawMessage `json:"config,omitempty"`
}

// Budget contains the limits of planned work on the shards and the nodes of a
// namespace. Resolve fills a nil field from the preset, else with the default in the
// comment of the field. An explicit zero is not nil, so Resolve keeps it.
//
// # Concurrency
//
// A copy of a Budget shares the values of its pointers and the elements of DrainRoles
// with the original.
type Budget struct {
	// MaxUnavailable is the most replicas of one shard that may be unavailable at
	// once. Failures, maintenance and transitions count against it. The default is 1,
	// because a move of primary-only takes its one replica down for the writer gap.
	// With an explicit zero, the budget does not admit planned unavailability.
	MaxUnavailable *int `json:"maxUnavailable,omitempty"`
	// MaxConcurrentNodes is the most nodes that admitted planned disruptions cover
	// at once while the nodes host a replica. A node without replicas does not
	// count. The default is 1.
	MaxConcurrentNodes *int `json:"maxConcurrentNodes,omitempty"`
	// MaxTransitions is the most transitions of a cell in flight at once. Repairs
	// count against it. The default is 64, and Resolve refuses a value below 1.
	MaxTransitions *int `json:"maxTransitions,omitempty"`
	// MaxNodeTransitions is the most transitions in flight at once that count against
	// one node. A transition counts against its destination and its fallback, and
	// against its source while the session of the source is live. The default is 16,
	// and Resolve refuses a value below 1.
	MaxNodeTransitions *int `json:"maxNodeTransitions,omitempty"`
	// NodeCallbacks is the most callbacks that the controller has outstanding to one
	// node at once. It does not bound the handlers that still run on the node after
	// the call of the controller has timed out. The default is the value of
	// MaxNodeTransitions, so that no callback waits for a permit. Resolve refuses a
	// value below 1.
	NodeCallbacks *int `json:"nodeCallbacks,omitempty"`
	// DrainRoles lists the roles whose replicas the core moves off a node before a
	// planned disruption of the node starts. Replicas in other roles remain on the
	// node and count as unavailable until the node returns. Resolve fills an empty
	// DrainRoles with the writer roles of the replica state model.
	DrainRoles []shard.Role `json:"drainRoles,omitempty"`
	// Interruption is the interruption of service that planned work may cause.
	// Resolve refuses every value other than PlannedInterruption.
	Interruption Interruption `json:"interruption,omitempty"`
}

// LoadModel lists the load metrics that placers balance, and the window over which
// agents average the load that they report.
//
// # Concurrency
//
// A copy of a LoadModel shares the elements of Metrics with the original.
type LoadModel struct {
	// Metrics lists the load metrics. No two metrics have the same name.
	Metrics []Metric `json:"metrics,omitempty"`
	// Window is the averaging window. The default is one minute.
	Window shard.Duration `json:"window,omitempty"`
}

// Metric is a load dimension of a node. Placers compare it with the capacity of the
// node whose name is Capacity, or with the capacity whose name is Name when Capacity
// is empty.
//
// # Concurrency
//
// A Metric is a value.
type Metric struct {
	// Name is the name under which agents report the metric, such as "cpu".
	Name string `json:"name"`
	// Capacity is the name of the capacity of a node that the metric fills, or empty
	// for the capacity named Name.
	Capacity string `json:"capacity,omitempty"`
}

// CellPolicy bounds the size of each cell of the namespace. The controller of each cell
// logs a warning when the cell rises above either bound.
//
// # Concurrency
//
// A CellPolicy is a value.
type CellPolicy struct {
	// MaxNodes is the most nodes of one cell. The default is 5,000.
	MaxNodes int `json:"maxNodes,omitempty"`
	// MaxShards is the most shards of one cell. The default is 1,000,000.
	MaxShards int `json:"maxShards,omitempty"`
}
