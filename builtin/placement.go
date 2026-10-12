// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/planner"
	"go.thesmos.sh/sharder/shard"
)

// The defaults of the configuration of the planner.
const (
	// defaultMaxMoves is the default of Search.MaxMoves.
	defaultMaxMoves = 500
	// defaultMaxEvaluations is the default of Search.MaxEvaluations.
	defaultMaxEvaluations = 1000000
	// defaultMaxSourceShards is the default of Search.MaxSourceShards.
	defaultMaxSourceShards = 5000
	// defaultSample is the default of Search.Sample.
	defaultSample = 16
	// defaultSearchTimeout is the default of Search.Timeout.
	defaultSearchTimeout = shard.Duration(time.Minute)
	// defaultMaxShardFraction is the default of Churn.MaxShardFraction.
	defaultMaxShardFraction = 0.09
	// defaultMovesPerNode is the default of Churn.PerNode.
	defaultMovesPerNode = 2
	// defaultMovesPerShard is the default of Churn.PerShard.
	defaultMovesPerShard = 1
	// defaultInterval is the default of Triggers.Interval.
	defaultInterval = shard.Duration(30 * time.Second)
	// defaultMinImbalance is the default of Triggers.MinImbalance.
	defaultMinImbalance = 1.1
	// defaultActiveAbove is the default of Triggers.ActiveAbove.
	defaultActiveAbove = 0.25
)

// The fields of a planner.Config that select strategies. A problem of a selection names
// its field and its index, such as constraints[1].
const (
	constraintsField = "constraints"
	goalsField       = "goals"
)

// Planner returns the factory of the planner, which parses a planner.Config. The planner
// has no constraints and no goals by default. The other defaults are these:
//
//   - search: maxMoves 500, maxEvaluations 1,000,000, maxSourceShards 5,000, sample 16
//     and a timeout of 60s.
//   - churn: maxShardFraction 0.09, perNode 2 and perShard 1.
//   - triggers: an interval of 30s, minImbalance 1.1, activeAbove 0.25 and onMembership
//     true.
//
// Parse parses the configuration of each constraint and goal with config.ParseNamed on
// r, and replaces it with the encoding of the result, which contains every default. New
// builds each constraint and goal with config.BuildNamed on r. The planner requires the
// capability controller from the topology.
//
// Error modes of Parse and New:
//   - an error that wraps ErrConfig and the error of the registry for each constraint or
//     goal that r cannot parse or build. Its text contains the index of the selection,
//     such as goals[1].
//   - an error that wraps ErrConfig for the configuration of a constraint or a goal that
//     does not encode.
//   - an error that wraps ErrConfig and the error of planner.New.
func Planner(r *config.Registry) config.Factory[placement.Placer] {
	return &factory[placement.Placer, planner.Config, *planner.Config]{
		name: planner.Name,
		defaults: func(c *planner.Config) {
			*c = planner.Config{
				Search: planner.Search{
					MaxMoves:        defaultMaxMoves,
					MaxEvaluations:  defaultMaxEvaluations,
					MaxSourceShards: defaultMaxSourceShards,
					Sample:          defaultSample,
					Timeout:         defaultSearchTimeout,
				},
				Churn: planner.Churn{
					MaxShardFraction: defaultMaxShardFraction,
					PerNode:          defaultMovesPerNode,
					PerShard:         defaultMovesPerShard,
				},
				Triggers: planner.Triggers{
					Interval:     defaultInterval,
					MinImbalance: defaultMinImbalance,
					ActiveAbove:  defaultActiveAbove,
					OnMembership: true,
				},
			}
		},
		selected: func(c *planner.Config) error {
			problems := make([]error, 0, len(c.Constraints)+len(c.Goals))
			for i := range c.Constraints {
				s := &c.Constraints[i]
				problems = append(problems, expand(r, config.Constraint, constraintsField, i, s.Name, &s.Config))
			}
			for i := range c.Goals {
				g := &c.Goals[i]
				problems = append(problems, expand(r, config.Goal, goalsField, i, g.Name, &g.Config))
			}
			return errors.Join(problems...)
		},
		capabilities: func(*planner.Config) config.Capabilities {
			return config.Capabilities{Requires: []config.Requirement{{
				Axis:       config.Topology,
				Capability: config.Controller,
				Reason:     "a controller runs the passes of the planner",
			}}}
		},
		build: func(c *planner.Config) (placement.Placer, error) {
			constraints := make([]placement.Constraint, len(c.Constraints))
			for i, s := range c.Constraints {
				cs, err := config.BuildNamed[placement.Constraint](r, config.Constraint, s.Name, s.Config)
				if err != nil {
					return nil, fmt.Errorf("%w: %s[%d]: %w", ErrConfig, constraintsField, i, err)
				}
				constraints[i] = cs
			}
			goals := make([]placement.Goal, len(c.Goals))
			for i, g := range c.Goals {
				gl, err := config.BuildNamed[placement.Goal](r, config.Goal, g.Name, g.Config)
				if err != nil {
					return nil, fmt.Errorf("%w: %s[%d]: %w", ErrConfig, goalsField, i, err)
				}
				goals[i] = gl
			}
			return built[placement.Placer](planner.New(c, constraints, goals))
		},
	}
}

// expand parses raw with the factory registered as name on axis in r, and replaces raw
// with the encoding of the parsed configuration, which contains every default. field
// and i are the field and the index of the selection in the configuration of the
// planner.
//
// Error modes:
//   - an error that wraps ErrConfig and the error of config.ParseNamed.
//   - an error that wraps ErrConfig and the error of encoding/json when the parsed
//     configuration does not encode.
func expand(
	r *config.Registry,
	axis config.Axis,
	field string,
	i int,
	name shard.StrategyName,
	raw *json.RawMessage,
) error {
	cfg, err := config.ParseNamed(r, axis, name, *raw)
	if err != nil {
		return fmt.Errorf("%w: %s[%d]: %w", ErrConfig, field, i, err)
	}
	encoded, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("%w: %s[%d]: %w", ErrConfig, field, i, err)
	}
	*raw = encoded
	return nil
}
