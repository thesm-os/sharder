// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/constraint"
	"go.thesmos.sh/sharder/placement/goal"
	"go.thesmos.sh/sharder/placement/planner"
	"go.thesmos.sh/sharder/shard"
)

// The configurations of the planner in the tests.
const (
	// selectionsConfig selects the constraint isolation and the goal utilisation, each
	// with a configuration that leaves out a field with a default.
	selectionsConfig = `{"constraints":[{"name":"` + string(constraint.IsolationName) +
		`","config":{"label":"zone"}}],"goals":[{"name":"` + string(goal.UtilisationName) +
		`","tier":1,"weight":2,"config":{"metric":"cpu"}}]}`
	// unknownConstraintConfig selects capacity and a constraint that the registries of
	// the tests do not have.
	unknownConstraintConfig = `{"constraints":[{"name":"` + string(constraint.CapacityName) + `"},{"name":"` +
		string(appConstraint) + `"}]}`
	// invalidGoalConfig selects the goal balance without the metric that it needs.
	invalidGoalConfig = `{"goals":[{"name":"` + string(goal.BalanceName) + `","tier":0,"weight":1}]}`
	// unencodableGoalConfig selects the goal whose configuration does not encode.
	unencodableGoalConfig = `{"goals":[{"name":"` + string(unencodableName) + `","tier":0,"weight":1}]}`
)

// unencodableName is the name of the goal whose configuration does not encode.
const unencodableName shard.StrategyName = "unencodable"

// errUnencodable is the error of the encoding of an unencodable configuration.
var errUnencodable = errors.New("builtin_test: the configuration does not encode")

// unencodable is a configuration whose encoding fails with errUnencodable.
type unencodable struct{}

// Validate accepts every unencodable configuration.
func (unencodable) Validate() error {
	return nil
}

// MarshalJSON returns errUnencodable.
func (unencodable) MarshalJSON() ([]byte, error) {
	return nil, errUnencodable
}

// unencodableGoal is a factory of goals whose Parse returns an unencodable
// configuration. Its other methods are those of the factory of count.
type unencodableGoal struct {
	config.Factory[placement.Goal]
}

// Name returns unencodableName.
func (unencodableGoal) Name() shard.StrategyName {
	return unencodableName
}

// Parse returns an unencodable configuration.
func (unencodableGoal) Parse(json.RawMessage) (config.Parsed, error) {
	return unencodable{}, nil
}

// TestPlacement checks the factory of the planner.
func TestPlacement(t *testing.T) {
	t.Parallel()

	t.Run("Planner", func(t *testing.T) {
		t.Parallel()

		t.Run("Parse", func(t *testing.T) {
			t.Parallel()

			t.Run("returns the defaults of the search, the churn and the triggers for no configuration",
				func(t *testing.T) {
					t.Parallel()
					assert.Equal(t, parse(t, builtin.Planner(newRegistry(t)), ""), config.Parsed(&planner.Config{
						Search: planner.Search{
							MaxMoves:        500,
							MaxEvaluations:  1000000,
							MaxSourceShards: 5000,
							Sample:          16,
							Timeout:         shard.Duration(time.Minute),
						},
						Churn: planner.Churn{MaxShardFraction: 0.09, PerNode: 2, PerShard: 1},
						Triggers: planner.Triggers{
							Interval:     shard.Duration(30 * time.Second),
							MinImbalance: 1.1,
							ActiveAbove:  0.25,
							OnMembership: true,
						},
					}), "Parse must return the defaults of the planner")
				})

			t.Run("expands the configuration of each constraint and goal with its defaults", func(t *testing.T) {
				t.Parallel()
				got := parse(t, builtin.Planner(newRegistry(t)), selectionsConfig).(*planner.Config)
				expect.Equal(t, got.Constraints, []planner.Selection{
					{Name: constraint.IsolationName, Config: json.RawMessage(`{"label":"zone","max":1}`)},
				}, "Parse must expand the configuration of each constraint")
				expect.Equal(t, got.Goals, []planner.GoalSelection{{
					Name:   goal.UtilisationName,
					Config: json.RawMessage(`{"metric":"cpu","above":0.9}`),
					Tier:   1,
					Weight: 2,
				}}, "Parse must expand the configuration of each goal")
			})

			t.Run("keeps an empty list of constraints", func(t *testing.T) {
				t.Parallel()
				got := parse(t, builtin.Planner(newRegistry(t)), `{"constraints":[]}`).(*planner.Config)
				assert.Equal(t, got.Constraints, []planner.Selection{}, "Parse must keep an empty list")
			})

			t.Run("returns ErrConfig with the index of a constraint that the registry does not have",
				func(t *testing.T) {
					t.Parallel()
					_, err := builtin.Planner(newRegistry(t)).Parse(json.RawMessage(unknownConstraintConfig))
					assert.ErrorIs(t, err, builtin.ErrConfig, "Parse must refuse a constraint without a factory")
					expect.That(t, err).ErrorIs(config.ErrUnknown, "Parse must wrap the error of the registry")
					expect.Contains(t, err.Error(), "constraints[1]", "the error must name the selection")
				})

			t.Run("returns ErrConfig with the index of a goal that its factory refuses", func(t *testing.T) {
				t.Parallel()
				_, err := builtin.Planner(newRegistry(t)).Parse(json.RawMessage(invalidGoalConfig))
				assert.ErrorIs(t, err, builtin.ErrConfig, "Parse must refuse a goal that its factory refuses")
				expect.That(t, err).ErrorIs(goal.ErrConfig, "Parse must wrap the error of the factory of the goal")
				expect.Contains(t, err.Error(), "goals[0]", "the error must name the selection")
			})

			t.Run("returns ErrConfig for the configuration of a goal that does not encode", func(t *testing.T) {
				t.Parallel()
				r := newRegistry(t)
				assert.NoError(t, config.Register(r, config.Goal, unencodableGoal{builtin.Count()}),
					"the registry must take the goal of the test")
				_, err := builtin.Planner(r).Parse(json.RawMessage(unencodableGoalConfig))
				assert.ErrorIs(t, err, builtin.ErrConfig, "Parse must refuse a configuration that does not encode")
				expect.That(t, err).ErrorIs(errUnencodable, "Parse must wrap the error of the encoding")
			})

			t.Run("returns the configuration of its own encoding", func(t *testing.T) {
				t.Parallel()
				f := builtin.Planner(newRegistry(t))
				roundTrip(t, f, parse(t, f, selectionsConfig))
			})
		})

		t.Run("Capabilities", func(t *testing.T) {
			t.Parallel()

			t.Run("requires a controller from the topology", func(t *testing.T) {
				t.Parallel()
				f := builtin.Planner(newRegistry(t))
				got := f.Capabilities(parse(t, f, "")).Requires
				assert.Length(t, got, 1, "the planner must have one requirement")
				expect.Equal(t, got[0].Axis, config.Topology, "the planner must require from the topology")
				expect.Equal(t, got[0].Capability, config.Controller, "the planner must require a controller")
			})
		})

		t.Run("New", func(t *testing.T) {
			t.Parallel()

			t.Run("builds the planner with the constraints and the goals of the configuration", func(t *testing.T) {
				t.Parallel()
				f := builtin.Planner(newRegistry(t))
				cfg := parse(t, f, selectionsConfig).(*planner.Config)
				got, err := f.New(cfg)
				assert.NoError(t, err, "New must build the planner")
				isolation, err := constraint.NewIsolation(constraint.IsolationConfig{Label: "zone", Max: 1})
				assert.NoError(t, err, "the isolation of the test must build")
				utilisation, err := goal.NewUtilisation(goal.UtilisationConfig{Metric: "cpu", Above: 0.9})
				assert.NoError(t, err, "the utilisation of the test must build")
				want, err := planner.New(cfg, []placement.Constraint{isolation}, []placement.Goal{utilisation})
				assert.NoError(t, err, "planner.New must build the planner of the test")
				assert.Equal(t, got, placement.Placer(want), "New must build the planner of its configuration")
			})

			tests := []struct {
				name  string
				give  *planner.Config
				where string
			}{
				{
					name:  "returns ErrConfig with the index of a constraint that the registry cannot build",
					give:  &planner.Config{Constraints: []planner.Selection{{Name: constraint.CapacityName}}},
					where: "constraints[0]",
				},
				{
					name:  "returns ErrConfig with the index of a goal that the registry cannot build",
					give:  &planner.Config{Goals: []planner.GoalSelection{{Name: goal.CountName, Weight: 1}}},
					where: "goals[0]",
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					_, err := builtin.Planner(config.NewRegistry(builtin.Support())).New(tt.give)
					assert.ErrorIs(t, err, builtin.ErrConfig, "New must refuse a selection without a factory")
					expect.That(t, err).ErrorIs(config.ErrUnknown, "New must wrap the error of the registry")
					expect.Contains(t, err.Error(), tt.where, "the error must name the selection")
				})
			}

			t.Run("returns ErrConfig for a configuration that planner.New refuses", func(t *testing.T) {
				t.Parallel()
				_, err := builtin.Planner(newRegistry(t)).New(&planner.Config{})
				assert.ErrorIs(t, err, builtin.ErrConfig, "New must refuse a configuration without limits")
				assert.ErrorIs(t, err, planner.ErrConfig, "New must wrap the error of planner.New")
			})
		})
	})
}
