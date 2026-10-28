// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package config_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/config/configtest"
	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/partition/singleton"
	"go.thesmos.sh/sharder/replication"
	"go.thesmos.sh/sharder/shard"
)

// The names of the test strategies and presets. singleton.Name and
// replication.PrimaryOnlyName are the names of the partitioner and the model.
const (
	plannerName         shard.StrategyName = "planner"
	balanceName         shard.StrategyName = "balance"
	exclusiveName       shard.StrategyName = "exclusive"
	unfencedName        shard.StrategyName = "exclusive-unfenced"
	overlappingName     shard.StrategyName = "overlapping"
	breakBeforeMakeName shard.StrategyName = "break-before-make"
	embeddedName        shard.StrategyName = "embedded"
	serviceName         shard.StrategyName = "service"
	watchName           shard.StrategyName = "watch"
	zoneName            shard.StrategyName = "zone"

	workerPreset config.PresetName = "exclusive-worker"
	testSpace    shard.Namespace   = "orders"
)

// The configurations of the test strategies.
const (
	// plannerConfig is the configuration of the planner in the preset of the tests.
	plannerConfig = `{"goals":["count"],"limit":5}`
	// emptyConfig is the configuration that a test factory parses from no JSON.
	emptyConfig = `{}`
)

// errTest is the error of the test factories and configurations that fail.
var errTest = errors.New("config_test: the test strategy fails")

// rawConfig is the configuration of a test strategy. It is the JSON that its factory
// parsed.
type rawConfig json.RawMessage

// Validate accepts every rawConfig.
func (rawConfig) Validate() error {
	return nil
}

// MarshalJSON returns the JSON that the factory parsed, so the resolved spec contains
// the configuration that the factory received.
func (c rawConfig) MarshalJSON() ([]byte, error) {
	return c, nil
}

// unencodableConfig is a configuration that encoding/json does not encode.
type unencodableConfig struct{}

// Validate accepts every unencodableConfig.
func (unencodableConfig) Validate() error {
	return nil
}

// MarshalJSON returns errTest.
func (unencodableConfig) MarshalJSON() ([]byte, error) {
	return nil, errTest
}

// testProvided lists the capabilities of the adapters of the tests.
var testProvided = map[config.Axis][]config.Capability{
	config.Store:      {config.Durable},
	config.Membership: {config.Leases},
}

// newSupport returns the support matrix of the tests. Placement is open, so a test can
// register a placer of its own. Every other axis of a spec is closed.
func newSupport() config.Support {
	return config.Support{
		Strategies: map[config.Axis][]shard.StrategyName{
			config.Partitioning: {singleton.Name},
			config.Replication:  {replication.PrimaryOnlyName},
			config.Ownership:    {exclusiveName, unfencedName},
			config.Handoff:      {breakBeforeMakeName},
			config.Topology:     {embeddedName, serviceName},
			config.Routing:      {watchName},
		},
		Open:     []config.Axis{config.Placement, config.Constraint, config.Goal, config.Hash},
		Requires: []config.Capability{config.Fenced},
	}
}

// newPreset returns the preset of the tests, which selects a strategy on every axis.
func newPreset() *config.Preset {
	return &config.Preset{
		Name:        workerPreset,
		Description: "exclusive workers with a fenced writer per shard",
		Spec: config.Spec{
			Partitioning: config.Selection{Name: singleton.Name},
			Placement:    config.Selection{Name: plannerName, Config: json.RawMessage(plannerConfig)},
			Replication:  config.Selection{Name: replication.PrimaryOnlyName},
			Ownership:    config.Selection{Name: exclusiveName},
			Handoff:      config.Selection{Name: breakBeforeMakeName},
			Topology:     config.Selection{Name: embeddedName},
			Routing:      config.Selection{Name: watchName},
			Budget:       config.Budget{Interruption: config.PlannedInterruption},
		},
	}
}

// newRegistry returns a registry of the support matrix s with the preset of newPreset.
// It registers a factory for every strategy of newSupport apart from serviceName. The
// factories declare the capabilities that the strategies of the release have.
func newRegistry(t *testing.T, s config.Support) *config.Registry {
	t.Helper()
	r := config.NewRegistry(s)
	controller := config.Requirement{
		Axis: config.Topology, Capability: config.Controller, Reason: "a controller commits the assignment",
	}
	register(t, r, config.Partitioning, newFactory(t, singleton.Name, config.Capabilities{},
		configtest.WithFactoryNew(func(config.Parsed) (partition.Partitioner, error) {
			return singleton.Partitioner{}, nil
		})))
	register(t, r, config.Placement, newFactory[struct{}](t, plannerName,
		config.Capabilities{Requires: []config.Requirement{controller}}))
	register(t, r, config.Placement, newFactory[struct{}](t, balanceName, config.Capabilities{}))
	register(t, r, config.Replication, newFactory(t, replication.PrimaryOnlyName,
		config.Capabilities{Provides: []config.Capability{config.SingleWriter}},
		configtest.WithFactoryNew(func(config.Parsed) (*replication.Compiled, error) {
			return replication.PrimaryOnly.Compile()
		})))
	register(t, r, config.Ownership, newFactory[struct{}](t, exclusiveName, config.Capabilities{
		Provides: []config.Capability{config.Fenced},
		Requires: []config.Requirement{
			{Axis: config.Replication, Capability: config.SingleWriter, Reason: "one tenure writes a shard"},
			{Axis: config.Handoff, Capability: config.ExclusiveSafe, Reason: "a move never runs two writers"},
			{Axis: config.Membership, Capability: config.Leases, Reason: "a lease ends a lost writer"},
		},
	}))
	register(t, r, config.Ownership, newFactory[struct{}](t, unfencedName, config.Capabilities{}))
	register(t, r, config.Handoff, newFactory[struct{}](t, breakBeforeMakeName,
		config.Capabilities{Provides: []config.Capability{config.ExclusiveSafe}}))
	register(t, r, config.Topology, newFactory[struct{}](t, embeddedName,
		config.Capabilities{Provides: []config.Capability{config.Controller}}))
	register(t, r, config.Routing, newFactory[struct{}](t, watchName,
		config.Capabilities{Requires: []config.Requirement{controller}}))
	assert.NoError(t, config.RegisterPreset(r, newPreset()), "the preset of the tests must register")
	return r
}

// newFactory returns a double of a factory of test strategies of type T. Its Name returns
// name, and its Capabilities returns caps. Its Parse returns raw as a rawConfig, and
// emptyConfig for no JSON. Its New returns the zero T. Each option of opts then changes the
// double, such as configtest.WithFactoryNew.
func newFactory[T any](
	tb assert.TB,
	name shard.StrategyName,
	caps config.Capabilities,
	opts ...configtest.FactoryStubOption[T],
) *configtest.FactoryStub[T] {
	tb.Helper()
	return configtest.NewFactoryStub(tb, append([]configtest.FactoryStubOption[T]{
		configtest.WithFactoryName[T](func() shard.StrategyName { return name }),
		configtest.WithFactoryParse[T](func(raw json.RawMessage) (config.Parsed, error) {
			if len(raw) == 0 {
				return rawConfig(emptyConfig), nil
			}
			return rawConfig(raw), nil
		}),
		configtest.WithFactoryCapabilities[T](func(config.Parsed) config.Capabilities { return caps }),
	}, opts...)...)
}

// register adds f to r on axis, and stops the test when Register fails.
func register[T any](t *testing.T, r *config.Registry, axis config.Axis, f config.Factory[T]) {
	t.Helper()
	assert.NoError(t, config.Register(r, axis, f), "a factory of the tests must register")
}

// TestRegistry checks the registration of factories, the strategies that Build and
// BuildNamed build with them, and the configurations that ParseNamed parses with them.
func TestRegistry(t *testing.T) {
	t.Parallel()

	t.Run("NewRegistry", func(t *testing.T) {
		t.Parallel()

		t.Run("keeps a copy of the support matrix", func(t *testing.T) {
			t.Parallel()
			s := newSupport()
			r := newRegistry(t, s)
			s.Strategies[config.Ownership][0] = overlappingName
			s.Strategies[config.Topology] = nil
			s.Open[0] = config.Hash
			s.Requires[0] = config.Durable
			res, err := config.Resolve(r, newSpec(workerPreset),
				map[config.Axis][]config.Capability{config.Membership: {config.Leases}})
			assert.NoError(t, err, "a change of the support matrix of the caller must not change the registry")
			assert.Equal(t, res.Spec.Ownership.Name, exclusiveName, "the registry must keep its own strategies")
		})
	})

	t.Run("Register", func(t *testing.T) {
		t.Parallel()

		t.Run("adds a factory that BuildNamed builds", func(t *testing.T) {
			t.Parallel()
			r := config.NewRegistry(newSupport())
			register(t, r, config.Constraint, newFactory(t, zoneName, config.Capabilities{},
				configtest.WithFactoryNew(func(cfg config.Parsed) (config.Parsed, error) {
					return cfg, nil
				})))
			got, err := config.BuildNamed[config.Parsed](r, config.Constraint, zoneName, json.RawMessage(plannerConfig))
			assert.NoError(t, err, "BuildNamed must build a registered constraint")
			assert.Equal(t, got, config.Parsed(rawConfig(plannerConfig)),
				"BuildNamed must build the strategy from the configuration that it parsed")
		})

		bounds := []struct {
			name string
			give config.Axis
		}{
			{name: "returns nil for the first axis of strategies", give: config.Partitioning},
			{name: "returns nil for the last axis of strategies", give: config.Hash},
		}
		for _, tt := range bounds {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				f := newFactory[struct{}](t, zoneName, config.Capabilities{})
				err := config.Register(config.NewRegistry(newSupport()), tt.give, f)
				assert.NoError(t, err, "Register must add a factory on every axis of strategies")
			})
		}

		t.Run("returns ErrDuplicate for a second factory of one name on one axis", func(t *testing.T) {
			t.Parallel()
			r := config.NewRegistry(newSupport())
			register(t, r, config.Goal, newFactory[struct{}](t, zoneName, config.Capabilities{}))
			err := config.Register(r, config.Goal, newFactory[struct{}](t, zoneName, config.Capabilities{}))
			assert.ErrorIs(t, err, config.ErrDuplicate, "Register must refuse a second factory of one name")
		})

		axes := []struct {
			name string
			give config.Axis
		}{
			{name: "returns ErrRegistration for the zero axis", give: 0},
			{name: "returns ErrRegistration for the role of the store", give: config.Store},
			{name: "returns ErrRegistration for the role of the membership", give: config.Membership},
		}
		for _, tt := range axes {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				f := newFactory[struct{}](t, zoneName, config.Capabilities{})
				err := config.Register(config.NewRegistry(newSupport()), tt.give, f)
				assert.ErrorIs(t, err, config.ErrRegistration, "Register must refuse an axis that takes no strategies")
			})
		}

		t.Run("returns ErrRegistration for a nil factory", func(t *testing.T) {
			t.Parallel()
			err := config.Register[struct{}](config.NewRegistry(newSupport()), config.Goal, nil)
			assert.ErrorIs(t, err, config.ErrRegistration, "Register must refuse a nil factory")
		})

		t.Run("returns ErrRegistration for a factory without a name", func(t *testing.T) {
			t.Parallel()
			err := config.Register(config.NewRegistry(newSupport()), config.Goal,
				newFactory[struct{}](t, "", config.Capabilities{}))
			assert.ErrorIs(t, err, config.ErrRegistration, "Register must refuse a factory without a name")
		})
	})

	t.Run("Build", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the strategy of the selection", func(t *testing.T) {
			t.Parallel()
			r := newRegistry(t, newSupport())
			res, err := config.Resolve(r, newSpec(workerPreset), testProvided)
			assert.NoError(t, err, "the spec of the test must resolve")
			got, err := config.Build[partition.Partitioner](r, res, config.Partitioning)
			assert.NoError(t, err, "Build must build the partitioner")
			assert.Equal(t, got, partition.Partitioner(singleton.Partitioner{}), "Build must return the singleton")
		})

		t.Run("returns ErrRegistration for an axis without a selection in a spec", func(t *testing.T) {
			t.Parallel()
			_, err := config.Build[struct{}](newRegistry(t, newSupport()), &config.Resolved{}, config.Constraint)
			assert.ErrorIs(t, err, config.ErrRegistration, "Build must refuse an axis without a selection")
		})

		t.Run("returns ErrUnknown for a selection without a factory", func(t *testing.T) {
			t.Parallel()
			res := &config.Resolved{Spec: config.Spec{Topology: config.Selection{Name: serviceName}}}
			_, err := config.Build[struct{}](newRegistry(t, newSupport()), res, config.Topology)
			assert.ErrorIs(t, err, config.ErrUnknown, "Build must refuse a name without a factory")
		})

		t.Run("returns ErrRegistration for a factory of another type", func(t *testing.T) {
			t.Parallel()
			res := &config.Resolved{Spec: config.Spec{Topology: config.Selection{Name: embeddedName}}}
			_, err := config.Build[partition.Partitioner](newRegistry(t, newSupport()), res, config.Topology)
			assert.ErrorIs(t, err, config.ErrRegistration, "Build must refuse a factory of another type")
		})

		t.Run("returns ErrSpec with the error of New", func(t *testing.T) {
			t.Parallel()
			r := newRegistry(t, newSupport())
			register(t, r, config.Placement, newFactory(t, zoneName, config.Capabilities{},
				configtest.WithFactoryNew(func(config.Parsed) (struct{}, error) {
					return struct{}{}, errTest
				})))
			res := &config.Resolved{Spec: config.Spec{Placement: config.Selection{Name: zoneName}}}
			_, err := config.Build[struct{}](r, res, config.Placement)
			assert.ErrorIs(t, err, config.ErrSpec, "Build must return ErrSpec for a New that fails")
			assert.ErrorIs(t, err, errTest, "Build must wrap the error of New")
		})
	})

	t.Run("BuildNamed", func(t *testing.T) {
		t.Parallel()

		failing := func(opts ...configtest.FactoryStubOption[struct{}]) *config.Registry {
			r := config.NewRegistry(newSupport())
			register(t, r, config.Goal, newFactory(t, zoneName, config.Capabilities{}, opts...))
			return r
		}
		invalid := configtest.NewParsedStub(t, configtest.WithParsedValidate(func() error { return errTest }))

		tests := []struct {
			name     string
			registry *config.Registry
			axis     config.Axis
			wantErrs []error
		}{
			{
				name:     "returns ErrRegistration for the zero axis",
				registry: failing(),
				axis:     0,
				wantErrs: []error{config.ErrRegistration},
			},
			{
				name:     "returns ErrRegistration for the role of the store",
				registry: failing(),
				axis:     config.Store,
				wantErrs: []error{config.ErrRegistration},
			},
			{
				name:     "returns ErrUnknown for a name without a factory",
				registry: config.NewRegistry(newSupport()),
				axis:     config.Goal,
				wantErrs: []error{config.ErrUnknown},
			},
			{
				name: "returns ErrSpec with the error of Parse",
				registry: failing(configtest.WithFactoryParse[struct{}](func(json.RawMessage) (config.Parsed, error) {
					return nil, errTest
				})),
				axis:     config.Goal,
				wantErrs: []error{config.ErrSpec, errTest},
			},
			{
				name: "returns ErrSpec when Parse returns no configuration",
				registry: failing(configtest.WithFactoryParse[struct{}](func(json.RawMessage) (config.Parsed, error) {
					return nil, nil
				})),
				axis:     config.Goal,
				wantErrs: []error{config.ErrSpec},
			},
			{
				name: "returns ErrSpec with the error of Validate",
				registry: failing(configtest.WithFactoryParse[struct{}](func(json.RawMessage) (config.Parsed, error) {
					return invalid, nil
				})),
				axis:     config.Goal,
				wantErrs: []error{config.ErrSpec, errTest},
			},
			{
				name: "returns ErrSpec with the error of New",
				registry: failing(configtest.WithFactoryNew(func(config.Parsed) (struct{}, error) {
					return struct{}{}, errTest
				})),
				axis:     config.Goal,
				wantErrs: []error{config.ErrSpec, errTest},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				_, err := config.BuildNamed[struct{}](tt.registry, tt.axis, zoneName, nil)
				for _, want := range tt.wantErrs {
					assert.ErrorIs(t, err, want, "BuildNamed must return the error of the case")
				}
				assert.Equal(t, errs.Classify(err), errs.Invalid, "the error of BuildNamed must classify as Invalid")
			})
		}

		bounds := []struct {
			name string
			give config.Axis
		}{
			{name: "builds a strategy on the first axis of strategies", give: config.Partitioning},
			{name: "builds a strategy on the last axis of strategies", give: config.Hash},
		}
		for _, tt := range bounds {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				r := config.NewRegistry(newSupport())
				register(t, r, tt.give, newFactory[struct{}](t, zoneName, config.Capabilities{}))
				_, err := config.BuildNamed[struct{}](r, tt.give, zoneName, nil)
				assert.NoError(t, err, "BuildNamed must build on every axis of strategies")
			})
		}

		t.Run("returns ErrRegistration for a factory of another type", func(t *testing.T) {
			t.Parallel()
			_, err := config.BuildNamed[partition.Partitioner](failing(), config.Goal, zoneName, nil)
			assert.ErrorIs(t, err, config.ErrRegistration, "BuildNamed must refuse a factory of another type")
		})

		t.Run("lets a factory build with the registry while Register waits for it", func(t *testing.T) {
			t.Parallel()
			r := config.NewRegistry(newSupport())
			register(t, r, config.Hash, newFactory[struct{}](t, zoneName, config.Capabilities{}))
			late := newFactory[struct{}](t, plannerName, config.Capabilities{})
			register(t, r, config.Goal, newFactory(t, balanceName, config.Capabilities{},
				configtest.WithFactoryNew(func(config.Parsed) (struct{}, error) {
					registered := make(chan error)
					go func() {
						registered <- config.Register(r, config.Hash, late)
					}()
					if err := <-registered; err != nil {
						return struct{}{}, err
					}
					return config.BuildNamed[struct{}](r, config.Hash, plannerName, nil)
				})))
			var err error
			assert.CompletesWithin(t, 5*time.Second, func(context.Context) error {
				_, err = config.BuildNamed[struct{}](r, config.Goal, balanceName, nil)
				return err
			}, "BuildNamed must call the factory without the lock of the registry")
			assert.NoError(t, err, "the factory must register and build while it builds")
		})
	})

	t.Run("ParseNamed", func(t *testing.T) {
		t.Parallel()

		withGoal := func(opts ...configtest.FactoryStubOption[struct{}]) *config.Registry {
			r := config.NewRegistry(newSupport())
			register(t, r, config.Goal, newFactory(t, zoneName, config.Capabilities{}, opts...))
			return r
		}
		invalid := configtest.NewParsedStub(t, configtest.WithParsedValidate(func() error { return errTest }))

		t.Run("returns the configuration that the factory parses", func(t *testing.T) {
			t.Parallel()
			got, err := config.ParseNamed(withGoal(), config.Goal, zoneName, json.RawMessage(plannerConfig))
			assert.NoError(t, err, "ParseNamed must parse the configuration of a registered goal")
			assert.Equal(t, got, config.Parsed(rawConfig(plannerConfig)),
				"ParseNamed must return the configuration that the factory parsed")
		})

		tests := []struct {
			name     string
			registry *config.Registry
			axis     config.Axis
			wantErrs []error
		}{
			{
				name:     "returns ErrRegistration for the zero axis",
				registry: withGoal(),
				axis:     0,
				wantErrs: []error{config.ErrRegistration},
			},
			{
				name:     "returns ErrRegistration for the role of the membership",
				registry: withGoal(),
				axis:     config.Membership,
				wantErrs: []error{config.ErrRegistration},
			},
			{
				name:     "returns ErrUnknown for a name without a factory",
				registry: config.NewRegistry(newSupport()),
				axis:     config.Goal,
				wantErrs: []error{config.ErrUnknown},
			},
			{
				name: "returns ErrSpec with the error of Parse",
				registry: withGoal(configtest.WithFactoryParse[struct{}](func(json.RawMessage) (config.Parsed, error) {
					return nil, errTest
				})),
				axis:     config.Goal,
				wantErrs: []error{config.ErrSpec, errTest},
			},
			{
				name: "returns ErrSpec when Parse returns no configuration",
				registry: withGoal(configtest.WithFactoryParse[struct{}](func(json.RawMessage) (config.Parsed, error) {
					return nil, nil
				})),
				axis:     config.Goal,
				wantErrs: []error{config.ErrSpec},
			},
			{
				name: "returns ErrSpec with the error of Validate",
				registry: withGoal(configtest.WithFactoryParse[struct{}](func(json.RawMessage) (config.Parsed, error) {
					return invalid, nil
				})),
				axis:     config.Goal,
				wantErrs: []error{config.ErrSpec, errTest},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				_, err := config.ParseNamed(tt.registry, tt.axis, zoneName, nil)
				for _, want := range tt.wantErrs {
					assert.ErrorIs(t, err, want, "ParseNamed must return the error of the case")
				}
				assert.Equal(t, errs.Classify(err), errs.Invalid, "the error of ParseNamed must classify as Invalid")
			})
		}

		bounds := []struct {
			name string
			give config.Axis
		}{
			{name: "parses a configuration on the first axis of strategies", give: config.Partitioning},
			{name: "parses a configuration on the last axis of strategies", give: config.Hash},
		}
		for _, tt := range bounds {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				r := config.NewRegistry(newSupport())
				register(t, r, tt.give, newFactory[struct{}](t, zoneName, config.Capabilities{}))
				_, err := config.ParseNamed(r, tt.give, zoneName, nil)
				assert.NoError(t, err, "ParseNamed must parse on every axis of strategies")
			})
		}
	})
}
