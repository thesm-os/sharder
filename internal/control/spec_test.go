// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package control_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/config/configtest"
	"go.thesmos.sh/sharder/handoff"
	"go.thesmos.sh/sharder/handoff/handofftest"
	"go.thesmos.sh/sharder/internal/control"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/placementtest"
	"go.thesmos.sh/sharder/placement/planner"
	"go.thesmos.sh/sharder/shard"
)

// The names of the strategies that the tests register.
const (
	// fragileName is the name of a strategy that does not build.
	fragileName shard.StrategyName = "fragile"
	// staticName is the name of a placer that is not the planner.
	staticName shard.StrategyName = "static"
	// nanName is the name of a goal whose every cost is NaN.
	nanName shard.StrategyName = "nan"
	// infName is the name of a goal whose every cost is infinite.
	infName shard.StrategyName = "inf"
)

// The configurations of the strategies of the tests.
const (
	// countOnly is the configuration of the planner with the goal count.
	countOnly = `{"goals":[{"name":"count","tier":0,"weight":1}]}`
	// fencedOnly is the configuration of exclusive ownership with every write fenced.
	fencedOnly = `{"fenced":true}`
	// fragileConstraint is the configuration of the planner with the constraint fragile.
	fragileConstraint = `{"constraints":[{"name":"fragile","config":{"max":1}}],` +
		`"goals":[{"name":"count","tier":0,"weight":1}]}`
	// fragileGoal is the configuration of the planner with the goal fragile.
	fragileGoal = `{"goals":[{"name":"fragile","tier":0,"weight":1}]}`
	// otherSlots is the configuration of fixed-slots with other slots than the tests.
	otherSlots = `{"slots":8}`
	// malformed is text that is not JSON.
	malformed = "{"
	// unknownPreset is a spec with a preset that the registry does not have.
	unknownPreset = `{"namespace":"orders","preset":"unknown","version":1}`
)

// errFragile is the error of each strategy that does not build.
var errFragile = errors.New("test: the strategy does not build")

// none is the configuration of a strategy of the tests without settings. encoding/json
// encodes it as an empty object, as the resolved spec contains it.
type none struct{}

// Validate returns nil.
func (none) Validate() error { return nil }

// TestSpec checks the resolution of the specs and the build of their strategies.
func TestSpec(t *testing.T) {
	t.Parallel()

	t.Run("Resolve", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the resolution of a spec", func(t *testing.T) {
			t.Parallel()
			raw, err := json.Marshal(testSpec(firstVersion))
			assert.NoError(t, err, "the spec of the tests must encode")
			res, err := control.Resolve(registry(t), provided, raw)
			assert.NoError(t, err, "Resolve must resolve the spec of the tests")
			expect.Equal(t, res.Spec.Version, firstVersion, "the resolution must have the version of the spec")
			expect.Equal(t, res.Mapping.Partitioner, fixedslots.Name,
				"the resolution must have the mapping of the spec")
		})

		t.Run("returns ErrSpec for a spec that does not decode", func(t *testing.T) {
			t.Parallel()
			_, err := control.Resolve(registry(t), provided, []byte(malformed))
			assert.ErrorIs(t, err, control.ErrSpec, "Resolve must refuse a spec that does not decode")
			_ = assert.ErrorAs[*json.SyntaxError](t, err, "the error must wrap the error of the decoder")
		})

		t.Run("returns ErrSpec for a spec that does not resolve", func(t *testing.T) {
			t.Parallel()
			_, err := control.Resolve(registry(t), provided, []byte(unknownPreset))
			expect.That(t, err).
				ErrorIs(control.ErrSpec, "Resolve must refuse a spec that does not resolve").
				ErrorIs(config.ErrUnknown, "the error must wrap the error of config.Resolve")
		})
	})

	t.Run("Build", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			give option
			name string
		}{
			{
				name: "returns ErrSpec for a placer that is not the planner",
				give: func(s *setup) {
					s.support.Strategies[config.Placement] = append(s.support.Strategies[config.Placement], staticName)
					s.extend = func(tb assert.TB, r *config.Registry) error {
						return config.Register(r, config.Placement, static(tb))
					}
					s.spec.Placement = config.Selection{Name: staticName}
				},
			},
			{
				name: "returns ErrSpec for a constraint that does not build",
				give: func(s *setup) {
					s.extend = func(tb assert.TB, r *config.Registry) error {
						return config.Register(r, config.Constraint, fragile(tb, builtin.MaxPerNode()))
					}
					s.spec.Placement = config.Selection{Name: planner.Name, Config: json.RawMessage(fragileConstraint)}
				},
			},
			{
				name: "returns ErrSpec for a goal that does not build",
				give: func(s *setup) {
					s.extend = func(tb assert.TB, r *config.Registry) error {
						return config.Register(r, config.Goal, fragile(tb, builtin.Count()))
					}
					s.spec.Placement = config.Selection{Name: planner.Name, Config: json.RawMessage(fragileGoal)}
				},
			},
			{
				name: "returns ErrSpec for a placer that does not build",
				give: selects(config.Placement, countOnly, func(tb assert.TB, r *config.Registry) error {
					return config.Register(r, config.Placement, fragile(tb, builtin.Planner(r)))
				}),
			},
			{
				name: "returns ErrSpec for a protocol that does not build",
				give: selects(config.Handoff, "", func(tb assert.TB, r *config.Registry) error {
					return config.Register(r, config.Handoff, fragile(tb, builtin.BreakBeforeMake()))
				}),
			},
			{
				name: "returns ErrSpec for an ownership that does not build",
				give: selects(config.Ownership, fencedOnly, func(tb assert.TB, r *config.Registry) error {
					return config.Register(r, config.Ownership, fragile(tb, builtin.Exclusive()))
				}),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				s := setup{spec: testSpec(firstVersion), support: builtin.Support()}
				tt.give(&s)
				r := config.NewRegistry(s.support)
				assert.NoError(t, builtin.Register(r), "the built-ins must register")
				assert.NoError(t, s.extend(t, r), "the strategies of the test must register")
				res, err := config.Resolve(r, &s.spec, provided)
				assert.NoError(t, err, "the spec of the test must resolve")
				_, err = control.Build(r, res)
				assert.ErrorIs(t, err, control.ErrSpec, "Build must refuse a spec whose strategies do not build")
			})
		}
	})
}

// testSpec returns the spec of the tests at the version v.
func testSpec(v epoch.Epoch) config.Spec {
	return config.Spec{
		Namespace:    namespace,
		Preset:       builtin.ExclusiveWorker,
		Partitioning: config.Selection{Name: fixedslots.Name, Config: json.RawMessage(slotsConfig)},
		Version:      v,
	}
}

// selects returns an option that registers a strategy with register, and selects it as
// fragileName on axis with the configuration raw.
func selects(axis config.Axis, raw string, register func(tb assert.TB, r *config.Registry) error) option {
	return func(s *setup) {
		s.support.Strategies[axis] = append(s.support.Strategies[axis], fragileName)
		s.extend = register
		sel := config.Selection{Name: fragileName}
		if raw != "" {
			sel.Config = json.RawMessage(raw)
		}
		switch axis {
		case config.Placement:
			s.spec.Placement = sel
		case config.Handoff:
			s.spec.Handoff = sel
		default:
			s.spec.Ownership = sel
		}
	}
}

// fragile returns a double of a factory named fragileName whose New returns errFragile. It
// parses and declares its configuration as the factory base does.
func fragile[T any](tb assert.TB, base config.Factory[T]) *configtest.FactoryStub[T] {
	tb.Helper()
	return configtest.NewFactoryStub(tb, configtest.FactoryStubDelegateTo(base),
		configtest.WithFactoryName[T](func() shard.StrategyName { return fragileName }),
		configtest.WithFactoryNew(func(config.Parsed) (T, error) {
			var zero T
			return zero, errFragile
		}))
}

// refusing returns a double of a factory of protocols named fragileName. It parses, declares
// and builds as base does, and each protocol that it builds refuses every transfer with a
// source with an error that wraps handoff.ErrTransfer.
func refusing(tb assert.TB, base config.Factory[handoff.Protocol]) *configtest.FactoryStub[handoff.Protocol] {
	tb.Helper()
	return configtest.NewFactoryStub(tb, configtest.FactoryStubDelegateTo(base),
		configtest.WithFactoryName[handoff.Protocol](func() shard.StrategyName { return fragileName }),
		configtest.WithFactoryNew(func(cfg config.Parsed) (handoff.Protocol, error) {
			p, err := base.New(cfg)
			if err != nil {
				return nil, err
			}
			return handofftest.NewProtocolStub(tb, handofftest.ProtocolStubDelegateTo(p),
				handofftest.WithProtocolScript(func(t handoff.Transfer) (handoff.Script, error) {
					if t.From != "" {
						return handoff.Script{}, fmt.Errorf("test: a transfer from %s: %w", t.From, handoff.ErrTransfer)
					}
					return p.Script(t)
				})), nil
		}))
}

// static returns a double of the factory of a placer other than the planner, named
// staticName. Its Parse returns the configuration without settings. A controller refuses
// such a placer before it builds one, so its New returns errFragile.
func static(tb assert.TB) *configtest.FactoryStub[placement.Placer] {
	tb.Helper()
	return configtest.NewFactoryStub(tb,
		configtest.WithFactoryName[placement.Placer](func() shard.StrategyName { return staticName }),
		configtest.WithFactoryParse[placement.Placer](func(json.RawMessage) (config.Parsed, error) {
			return none{}, nil
		}),
		configtest.WithFactoryNew(func(config.Parsed) (placement.Placer, error) { return nil, errFragile }))
}

// badCost returns a double of the factory of a goal named name, whose every cost is cost
// and whose every delta is 0. The Parse of the factory returns the configuration without
// settings.
func badCost(tb assert.TB, name shard.StrategyName, cost float64) *configtest.FactoryStub[placement.Goal] {
	tb.Helper()
	goal := placementtest.NewGoalStub(tb,
		placementtest.WithGoalName(func() shard.StrategyName { return name }),
		placementtest.WithGoalCost(func(*placement.State) float64 { return cost }))
	return configtest.NewFactoryStub(tb,
		configtest.WithFactoryName[placement.Goal](func() shard.StrategyName { return name }),
		configtest.WithFactoryParse[placement.Goal](func(json.RawMessage) (config.Parsed, error) {
			return none{}, nil
		}),
		configtest.WithFactoryNew(func(config.Parsed) (placement.Goal, error) { return goal, nil }))
}
