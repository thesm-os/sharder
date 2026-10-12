// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin

import (
	"encoding/json"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/handoff/breakbeforemake"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/partition/singleton"
	"go.thesmos.sh/sharder/placement/planner"
	"go.thesmos.sh/sharder/replication"
)

// The names of the presets of the support matrix.
const (
	// ExclusiveWorker is the preset of stream processors, schedulers and per-tenant
	// workers.
	ExclusiveWorker config.PresetName = "exclusive-worker"
	// ConsumerGroup is the preset of the fixed partitions of a queue.
	ConsumerGroup config.PresetName = "consumer-group"
	// ActorEntities is the preset of entity and actor sharding.
	ActorEntities config.PresetName = "actor-entities"
	// LeaderElection is the preset of one active instance of a component.
	LeaderElection config.PresetName = "leader-election"
)

// The configurations that the presets select, in the JSON of a spec.
const (
	// fencedConfig is the configuration of exclusive ownership in every preset.
	fencedConfig = `{"fenced":true}`
	// workerSlotsConfig is the configuration of fixed-slots in exclusive-worker.
	workerSlotsConfig = `{"slots":1024}`
	// countConfig is the configuration of the planner in exclusive-worker and
	// actor-entities. It balances the number of replicas of each node.
	countConfig = `{"goals":[{"name":"count","tier":0,"weight":1}]}`
	// stickyCountConfig is the configuration of the planner in consumer-group. It
	// balances the number of replicas of each node, and then keeps each replica where
	// it is.
	stickyCountConfig = `{"goals":[{"name":"count","tier":0,"weight":1},{"name":"stickiness","tier":1,"weight":1}]}`
)

// Presets returns the four presets of the support matrix, in the order ExclusiveWorker,
// ConsumerGroup, ActorEntities and LeaderElection. Each of them selects primary-only,
// exclusive with "fenced": true, break-before-make, embedded and watch, and a budget
// with planned interruption. They differ in their partitioning and their placement:
//
//   - ExclusiveWorker selects fixed-slots with 1,024 slots, and the planner with the
//     goal count.
//   - ConsumerGroup selects fixed-slots with the default slots, and the planner with
//     the goal count in tier 0 and the goal stickiness in tier 1.
//   - ActorEntities selects fixed-slots with the default slots, and the planner with the
//     goal count.
//   - LeaderElection selects singleton, and the planner without goals.
//
// Each call returns new presets, so config.RegisterPreset can keep them.
func Presets() []config.Preset {
	return []config.Preset{
		{
			Name:        ExclusiveWorker,
			Description: "Stream processors, schedulers and per-tenant workers, with one fenced writer per shard.",
			Spec: presetSpec(
				config.Selection{Name: fixedslots.Name, Config: json.RawMessage(workerSlotsConfig)},
				config.Selection{Name: planner.Name, Config: json.RawMessage(countConfig)},
			),
		},
		{
			Name: ConsumerGroup,
			Description: "The fixed partitions of a queue, with one fenced consumer per partition. " +
				"Set one slot per partition.",
			Spec: presetSpec(
				config.Selection{Name: fixedslots.Name},
				config.Selection{Name: planner.Name, Config: json.RawMessage(stickyCountConfig)},
			),
		},
		{
			Name:        ActorEntities,
			Description: "Entities and actors, with one fenced owner per shard. Set about 10 slots per expected node.",
			Spec: presetSpec(
				config.Selection{Name: fixedslots.Name},
				config.Selection{Name: planner.Name, Config: json.RawMessage(countConfig)},
			),
		},
		{
			Name:        LeaderElection,
			Description: "One active instance of a component, which is the fenced writer of the one shard.",
			Spec:        presetSpec(config.Selection{Name: singleton.Name}, config.Selection{Name: planner.Name}),
		},
	}
}

// presetSpec returns the spec of a preset whose partitioning and placement are the
// selections part and place. On every other axis, the spec selects the strategy of the
// support matrix.
func presetSpec(part, place config.Selection) config.Spec {
	return config.Spec{
		Partitioning: part,
		Placement:    place,
		Replication:  config.Selection{Name: replication.PrimaryOnlyName},
		Ownership:    config.Selection{Name: ExclusiveName, Config: json.RawMessage(fencedConfig)},
		Handoff:      config.Selection{Name: breakbeforemake.Name},
		Topology:     config.Selection{Name: EmbeddedName},
		Routing:      config.Selection{Name: WatchName},
		Budget:       config.Budget{Interruption: config.PlannedInterruption},
	}
}
