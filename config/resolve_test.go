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
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/partition/singleton"
	"go.thesmos.sh/sharder/replication"
	"go.thesmos.sh/sharder/shard"
)

// The labels of the draws of the properties of Resolve.
const (
	drawCount = "count"
	drawValue = "value"
)

// The names of the metrics and the presets of the tests of Resolve.
const (
	cpuMetric    = "cpu"
	bytesMetric  = "bytes"
	diskCapacity = "disk"

	tunedPreset config.PresetName = "tuned"
	mergePreset config.PresetName = "merge"
	barePreset  config.PresetName = "bare"
)

// The places in a spec that the errors of Resolve contain.
const (
	placeWindow   = "load.window"
	placeMaxNodes = "cells.maxNodes"
)

// workerSpec returns a spec of the namespace of the tests in which only the namespace
// and the preset are set.
func workerSpec() *config.Spec {
	return &config.Spec{Namespace: testSpace, Preset: workerPreset}
}

// TestResolve checks the resolution of specs over their presets, and every problem
// that Resolve reports.
func TestResolve(t *testing.T) {
	t.Parallel()

	t.Run("Resolve", func(t *testing.T) {
		t.Parallel()

		t.Run("fills every field that the spec leaves empty", func(t *testing.T) {
			t.Parallel()
			spec := workerSpec()
			spec.Version = 3
			res, err := config.Resolve(newRegistry(t, newSupport()), spec, testProvided)
			assert.NoError(t, err, "the spec of the preset must resolve")
			empty := json.RawMessage(emptyConfig)
			want := config.Spec{
				Namespace:    testSpace,
				Preset:       workerPreset,
				Version:      3,
				Partitioning: config.Selection{Name: singleton.Name, Config: empty},
				Placement:    config.Selection{Name: plannerName, Config: json.RawMessage(plannerConfig)},
				Replication:  config.Selection{Name: replication.PrimaryOnlyName, Config: empty},
				Ownership:    config.Selection{Name: exclusiveName, Config: empty},
				Handoff:      config.Selection{Name: breakBeforeMakeName, Config: empty},
				Topology:     config.Selection{Name: embeddedName, Config: empty},
				Routing:      config.Selection{Name: watchName, Config: empty},
				Budget: config.Budget{
					MaxUnavailable:     new(1),
					MaxConcurrentNodes: new(1),
					MaxTransitions:     new(64),
					MaxNodeTransitions: new(16),
					NodeCallbacks:      new(16),
					DrainRoles:         []shard.Role{shard.RolePrimary},
					Interruption:       config.PlannedInterruption,
				},
				Load:  config.LoadModel{Window: shard.Duration(time.Minute)},
				Cells: config.CellPolicy{MaxNodes: 5000, MaxShards: 1000000},
			}
			expect.Equal(t, res.Spec, want, "Resolve must fill every field from the preset and the defaults")
			expect.Equal(t, res.Mapping, singleton.Partitioner{}.Mapping(),
				"Resolve must read the mapping of the partitioner that it builds")
			expect.Length(t, res.Configs, 7, "Resolve must parse one configuration per axis of a spec")
			expect.Equal(t, res.Configs[config.Placement], config.Parsed(rawConfig(plannerConfig)),
				"Resolve must keep the configuration that the factory parsed")
		})

		t.Run("keeps the values of the spec over the preset", func(t *testing.T) {
			t.Parallel()
			spec := workerSpec()
			spec.Budget = config.Budget{
				MaxUnavailable:     new(0),
				MaxConcurrentNodes: new(2),
				MaxTransitions:     new(8),
				MaxNodeTransitions: new(4),
				NodeCallbacks:      new(3),
				DrainRoles:         []shard.Role{shard.RolePrimary},
				Interruption:       config.PlannedInterruption,
			}
			spec.Load = config.LoadModel{
				Metrics: []config.Metric{{Name: cpuMetric}, {Name: bytesMetric, Capacity: diskCapacity}},
				Window:  shard.Duration(30 * time.Second),
			}
			spec.Cells = config.CellPolicy{MaxNodes: 10, MaxShards: 100}
			res, err := config.Resolve(newRegistry(t, newSupport()), spec, testProvided)
			assert.NoError(t, err, "the spec must resolve")
			expect.Equal(t, res.Spec.Budget, spec.Budget, "Resolve must keep the budget of the spec")
			expect.Equal(t, res.Spec.Load, spec.Load, "Resolve must keep the load model of the spec")
			expect.Equal(t, res.Spec.Cells, spec.Cells, "Resolve must keep the cell bounds of the spec")
		})

		t.Run("takes the values of the preset over the defaults", func(t *testing.T) {
			t.Parallel()
			r := newRegistry(t, newSupport())
			p := newPreset()
			p.Name = tunedPreset
			p.Spec.Budget = config.Budget{
				MaxUnavailable:     new(0),
				MaxConcurrentNodes: new(3),
				MaxTransitions:     new(9),
				MaxNodeTransitions: new(5),
				NodeCallbacks:      new(2),
				DrainRoles:         []shard.Role{shard.RolePrimary},
				Interruption:       config.PlannedInterruption,
			}
			p.Spec.Load = config.LoadModel{
				Metrics: []config.Metric{{Name: cpuMetric}},
				Window:  shard.Duration(time.Second),
			}
			p.Spec.Cells = config.CellPolicy{MaxNodes: 20, MaxShards: 200}
			assert.NoError(t, config.RegisterPreset(r, p), "the preset must register")
			res, err := config.Resolve(r, &config.Spec{Namespace: testSpace, Preset: tunedPreset}, testProvided)
			assert.NoError(t, err, "the spec must resolve")
			expect.Equal(t, res.Spec.Budget, p.Spec.Budget, "Resolve must take the budget of the preset")
			expect.Equal(t, res.Spec.Load, p.Spec.Load, "Resolve must take the load model of the preset")
			expect.Equal(t, res.Spec.Cells, p.Spec.Cells, "Resolve must take the cell bounds of the preset")
		})

		t.Run("takes NodeCallbacks from MaxNodeTransitions", func(t *testing.T) {
			t.Parallel()
			spec := workerSpec()
			spec.Budget.MaxNodeTransitions = new(8)
			res, err := config.Resolve(newRegistry(t, newSupport()), spec, testProvided)
			assert.NoError(t, err, "the spec must resolve")
			assert.Equal(t, *res.Spec.Budget.NodeCallbacks, 8, "NodeCallbacks must default to MaxNodeTransitions")
		})

		t.Run("returns ErrSpec for a count of the budget below its least value", func(t *testing.T) {
			t.Parallel()
			r := newRegistry(t, newSupport())
			counts := []struct {
				set   func(b *config.Budget, v *int)
				least int
			}{
				{set: func(b *config.Budget, v *int) { b.MaxUnavailable = v }, least: 0},
				{set: func(b *config.Budget, v *int) { b.MaxConcurrentNodes = v }, least: 0},
				{set: func(b *config.Budget, v *int) { b.MaxTransitions = v }, least: 1},
				{set: func(b *config.Budget, v *int) { b.MaxNodeTransitions = v }, least: 1},
				{set: func(b *config.Budget, v *int) { b.NodeCallbacks = v }, least: 1},
			}
			prop.ForAll(t, "Resolve accepts a count of the budget exactly when it is not below its least value",
				func(c *prop.Case) {
					count := counts[c.Draw(prop.Integer(0, len(counts)-1), drawCount)]
					v := c.Draw(prop.Integer(-3, 3), drawValue)
					spec := workerSpec()
					count.set(&spec.Budget, &v)
					_, err := config.Resolve(r, spec, testProvided)
					if v < count.least {
						assert.ErrorIs(c, err, config.ErrSpec, "Resolve must refuse a count below its least value")
						return
					}
					assert.NoError(c, err, "Resolve must accept a count at or above its least value")
				})
		})

		t.Run("returns ErrSpec for an interruption that is not planned", func(t *testing.T) {
			t.Parallel()
			spec := workerSpec()
			spec.Budget.Interruption = config.PlannedInterruption + 1
			_, err := config.Resolve(newRegistry(t, newSupport()), spec, testProvided)
			assert.ErrorIs(t, err, config.ErrSpec, "Resolve must refuse an interruption other than planned")
		})

		t.Run("returns ErrSpec for a resolved spec without an interruption", func(t *testing.T) {
			t.Parallel()
			r := newRegistry(t, newSupport())
			p := newPreset()
			p.Name = barePreset
			p.Spec.Budget.Interruption = 0
			assert.NoError(t, config.RegisterPreset(r, p), "the preset must register")
			_, err := config.Resolve(r, &config.Spec{Namespace: testSpace, Preset: barePreset}, testProvided)
			assert.ErrorIs(t, err, config.ErrSpec, "Resolve must refuse a namespace without an interruption")
		})

		t.Run("returns ErrSpec for a negative load window", func(t *testing.T) {
			t.Parallel()
			spec := workerSpec()
			spec.Load.Window = -1
			_, err := config.Resolve(newRegistry(t, newSupport()), spec, testProvided)
			assert.ErrorIs(t, err, config.ErrSpec, "Resolve must refuse a negative window")
		})

		t.Run("returns ErrSpec for a metric without a name", func(t *testing.T) {
			t.Parallel()
			spec := workerSpec()
			spec.Load.Metrics = []config.Metric{{Capacity: diskCapacity}}
			_, err := config.Resolve(newRegistry(t, newSupport()), spec, testProvided)
			assert.ErrorIs(t, err, config.ErrSpec, "Resolve must refuse a metric without a name")
		})

		t.Run("returns ErrSpec for a metric listed twice", func(t *testing.T) {
			t.Parallel()
			spec := workerSpec()
			spec.Load.Metrics = []config.Metric{{Name: cpuMetric}, {Name: bytesMetric}, {Name: cpuMetric}}
			_, err := config.Resolve(newRegistry(t, newSupport()), spec, testProvided)
			assert.ErrorIs(t, err, config.ErrSpec, "Resolve must refuse a metric listed twice")
		})

		t.Run("returns ErrSpec for a negative most nodes of a cell", func(t *testing.T) {
			t.Parallel()
			spec := workerSpec()
			spec.Cells.MaxNodes = -1
			_, err := config.Resolve(newRegistry(t, newSupport()), spec, testProvided)
			assert.ErrorIs(t, err, config.ErrSpec, "Resolve must refuse a negative bound of nodes")
		})

		t.Run("returns ErrSpec for a negative most shards of a cell", func(t *testing.T) {
			t.Parallel()
			spec := workerSpec()
			spec.Cells.MaxShards = -1
			_, err := config.Resolve(newRegistry(t, newSupport()), spec, testProvided)
			assert.ErrorIs(t, err, config.ErrSpec, "Resolve must refuse a negative bound of shards")
		})

		t.Run("returns ErrSpec for a spec without a namespace", func(t *testing.T) {
			t.Parallel()
			_, err := config.Resolve(newRegistry(t, newSupport()), &config.Spec{Preset: workerPreset}, testProvided)
			assert.ErrorIs(t, err, config.ErrSpec, "Resolve must refuse a spec without a namespace")
		})

		t.Run("returns ErrSpec for a spec without a preset", func(t *testing.T) {
			t.Parallel()
			spec := newPreset().Spec
			spec.Namespace = testSpace
			_, err := config.Resolve(newRegistry(t, newSupport()), &spec, testProvided)
			expect.ErrorIs(t, err, config.ErrSpec, "Resolve must refuse a spec without a preset")
			expect.ErrorIsNot(t, err, config.ErrUnknown, "Resolve must not look up a preset without a name")
		})

		t.Run("returns ErrUnknown for a preset that the registry does not have", func(t *testing.T) {
			t.Parallel()
			spec := &config.Spec{Namespace: testSpace, Preset: tunedPreset}
			_, err := config.Resolve(newRegistry(t, newSupport()), spec, testProvided)
			assert.ErrorIs(t, err, config.ErrUnknown, "Resolve must refuse an unknown preset")
		})

		t.Run("returns ErrSpec for an axis without a strategy", func(t *testing.T) {
			t.Parallel()
			r := newRegistry(t, newSupport())
			p := newPreset()
			p.Name = barePreset
			p.Spec.Routing = config.Selection{}
			assert.NoError(t, config.RegisterPreset(r, p), "the preset must register")
			_, err := config.Resolve(r, &config.Spec{Namespace: testSpace, Preset: barePreset}, testProvided)
			assert.ErrorIs(t, err, config.ErrSpec, "Resolve must refuse an axis without a strategy")
		})

		t.Run("returns ErrUnsupported for a selection outside the support matrix", func(t *testing.T) {
			t.Parallel()
			spec := workerSpec()
			spec.Ownership = config.Selection{Name: overlappingName}
			_, err := config.Resolve(newRegistry(t, newSupport()), spec, testProvided)
			expect.ErrorIs(t, err, config.ErrUnsupported, "Resolve must refuse a selection outside the matrix")
			expect.ErrorIs(t, err, errors.ErrUnsupported, "the error must match errors.ErrUnsupported")
			expect.Equal(t, errs.Classify(err), errs.Unsupported, "the error must classify as Unsupported")
		})

		t.Run("returns only the problems with the support matrix when it has them", func(t *testing.T) {
			t.Parallel()
			spec := workerSpec()
			spec.Ownership = config.Selection{Name: overlappingName}
			spec.Load.Window = -1
			_, err := config.Resolve(newRegistry(t, newSupport()), spec, testProvided)
			expect.ErrorIs(t, err, config.ErrUnsupported, "Resolve must return the problem with the matrix")
			expect.ErrorIsNot(t, err, config.ErrSpec, "Resolve must leave out the other problems")
		})

		t.Run("selects a registered name on an open axis that the support matrix does not list", func(t *testing.T) {
			t.Parallel()
			spec := workerSpec()
			spec.Placement = config.Selection{Name: balanceName}
			res, err := config.Resolve(newRegistry(t, newSupport()), spec, testProvided)
			assert.NoError(t, err, "Resolve must accept a registered name on an open axis")
			assert.Equal(t, res.Spec.Placement.Name, balanceName, "Resolve must select the placer of the spec")
		})

		t.Run("returns ErrUnknown for a supported name without a factory", func(t *testing.T) {
			t.Parallel()
			spec := workerSpec()
			spec.Topology = config.Selection{Name: serviceName}
			_, err := config.Resolve(newRegistry(t, newSupport()), spec, testProvided)
			assert.ErrorIs(t, err, config.ErrUnknown, "Resolve must refuse a name without a factory")
		})

		parsing := []struct {
			parse    func(json.RawMessage) (config.Parsed, error)
			name     string
			wantErrs []error
		}{
			{
				name: "returns ErrSpec with the error of Parse",
				parse: func(json.RawMessage) (config.Parsed, error) {
					return nil, errTest
				},
				wantErrs: []error{config.ErrSpec, errTest},
			},
			{
				name: "returns ErrSpec when the factory parses no configuration",
				parse: func(json.RawMessage) (config.Parsed, error) {
					return nil, nil
				},
				wantErrs: []error{config.ErrSpec},
			},
			{
				name: "returns ErrSpec with the error of Validate",
				parse: func(json.RawMessage) (config.Parsed, error) {
					return invalidConfig{}, nil
				},
				wantErrs: []error{config.ErrSpec, errTest},
			},
			{
				name: "returns ErrSpec for a configuration that does not encode in JSON",
				parse: func(json.RawMessage) (config.Parsed, error) {
					return unencodableConfig{}, nil
				},
				wantErrs: []error{config.ErrSpec, errTest},
			},
		}
		for _, tt := range parsing {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				r := newRegistry(t, newSupport())
				register(t, r, config.Placement, &testFactory[struct{}]{name: zoneName, parse: tt.parse})
				spec := workerSpec()
				spec.Placement = config.Selection{Name: zoneName}
				_, err := config.Resolve(r, spec, testProvided)
				for _, want := range tt.wantErrs {
					expect.ErrorIs(t, err, want, "Resolve must return the error of the factory")
				}
				expect.Equal(t, errs.Classify(err), errs.Invalid, "the error must classify as Invalid")
			})
		}

		merges := []struct {
			name   string
			target string
			patch  string
			want   string
		}{
			{name: "replaces a member", target: `{"a":"b"}`, patch: `{"a":"c"}`, want: `{"a":"c"}`},
			{name: "adds a member", target: `{"a":"b"}`, patch: `{"b":"c"}`, want: `{"a":"b","b":"c"}`},
			{name: "removes a member", target: `{"a":"b"}`, patch: `{"a":null}`, want: `{}`},
			{name: "removes one of two members", target: `{"a":"b","b":"c"}`, patch: `{"a":null}`, want: `{"b":"c"}`},
			{name: "replaces an array with a string", target: `{"a":["b"]}`, patch: `{"a":"c"}`, want: `{"a":"c"}`},
			{name: "replaces a string with an array", target: `{"a":"c"}`, patch: `{"a":["b"]}`, want: `{"a":["b"]}`},
			{
				name:   "merges a nested object",
				target: `{"a":{"b":"c"}}`, patch: `{"a":{"b":"d","c":null}}`, want: `{"a":{"b":"d"}}`,
			},
			{
				name:   "replaces an array of objects",
				target: `{"a":[{"b":"c"}]}`, patch: `{"a":[1]}`, want: `{"a":[1]}`,
			},
			{name: "replaces an array with an array", target: `["a","b"]`, patch: `["c","d"]`, want: `["c","d"]`},
			{name: "replaces an object with an array", target: `{"a":"b"}`, patch: `["c"]`, want: `["c"]`},
			{
				name:   "yields the default configuration for a patch of null",
				target: `{"a":"foo"}`, patch: `null`, want: emptyConfig,
			},
			{name: "replaces an object with a string", target: `{"a":"foo"}`, patch: `"bar"`, want: `"bar"`},
			{
				name:   "keeps a null member of the target",
				target: `{"e":null}`, patch: `{"a":1}`, want: `{"e":null,"a":1}`,
			},
			{
				name:   "replaces an array with an object without its null members",
				target: `[1,2]`, patch: `{"a":"b","c":null}`, want: `{"a":"b"}`,
			},
			{
				name:   "adds a nested object without its null members",
				target: `{}`, patch: `{"a":{"bb":{"ccc":null}}}`, want: `{"a":{"bb":{}}}`,
			},
			{
				name: "applies the example of section 3 of RFC 7396",
				target: `{"title":"Goodbye!","author":{"givenName":"John","familyName":"Doe"},` +
					`"tags":["example","sample"],"content":"This will be unchanged"}`,
				patch: `{"title":"Hello!","phoneNumber":"+01-123-456-7890","author":{"familyName":null},` +
					`"tags":["example"]}`,
				want: `{"title":"Hello!","author":{"givenName":"John"},"tags":["example"],` +
					`"content":"This will be unchanged","phoneNumber":"+01-123-456-7890"}`,
			},
		}
		for _, tt := range merges {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				res, err := resolveMerge(t, tt.target, tt.patch)
				assert.NoError(t, err, "the spec must resolve")
				var got, want any
				assert.NoError(t, json.Unmarshal(res.Spec.Placement.Config, &got), "the result must be JSON")
				assert.NoError(t, json.Unmarshal([]byte(tt.want), &want), "the expected result must be JSON")
				assert.Equal(t, got, want, "Resolve must return the result that RFC 7396 gives for the example")
			})
		}

		t.Run("keeps the text of every number of a merge", func(t *testing.T) {
			t.Parallel()
			res, err := resolveMerge(t, `{"big":1}`, `{"big":18446744073709551615,"f":1.50}`)
			assert.NoError(t, err, "the spec must resolve")
			assert.Equal(t, string(res.Spec.Placement.Config), `{"big":18446744073709551615,"f":1.50}`,
				"Resolve must keep the text of each number")
		})

		t.Run("merges a patch over a preset without a configuration", func(t *testing.T) {
			t.Parallel()
			spec := workerSpec()
			spec.Partitioning = config.Selection{Config: json.RawMessage(`{"a":{"b":null},"c":1}`)}
			res, err := config.Resolve(newRegistry(t, newSupport()), spec, testProvided)
			assert.NoError(t, err, "the spec must resolve")
			assert.Equal(t, string(res.Spec.Partitioning.Config), `{"a":{},"c":1}`,
				"Resolve must merge the patch over an absent configuration")
		})

		t.Run("merges a selection that names the strategy of the preset", func(t *testing.T) {
			t.Parallel()
			spec := workerSpec()
			spec.Placement = config.Selection{Name: plannerName, Config: json.RawMessage(`{"limit":6}`)}
			res, err := config.Resolve(newRegistry(t, newSupport()), spec, testProvided)
			assert.NoError(t, err, "the spec must resolve")
			assert.Equal(t, string(res.Spec.Placement.Config), `{"goals":["count"],"limit":6}`,
				"Resolve must merge the selection over the configuration of the preset")
		})

		t.Run("starts a selection of another strategy from its defaults", func(t *testing.T) {
			t.Parallel()
			spec := workerSpec()
			spec.Placement = config.Selection{Name: balanceName, Config: json.RawMessage(`{"x":1}`)}
			res, err := config.Resolve(newRegistry(t, newSupport()), spec, testProvided)
			assert.NoError(t, err, "the spec must resolve")
			assert.Equal(t, string(res.Spec.Placement.Config), `{"x":1}`,
				"Resolve must not merge the configuration of the preset into another strategy")
		})

		notJSON := []struct {
			name   string
			target string
			patch  string
		}{
			{name: "returns ErrSpec with the syntax error of a patch", target: plannerConfig, patch: `{"a" x}`},
			{name: "returns ErrSpec with the syntax error of a target", target: `{"a" x}`, patch: `{}`},
		}
		for _, tt := range notJSON {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				_, err := resolveMerge(t, tt.target, tt.patch)
				expect.ErrorIs(t, err, config.ErrSpec, "Resolve must refuse a configuration that is not JSON")
				_ = expect.ErrorAs[*json.SyntaxError](t, err, "Resolve must wrap the syntax error of encoding/json")
			})
		}

		t.Run("returns ErrSpec for a patch with data after its value", func(t *testing.T) {
			t.Parallel()
			_, err := resolveMerge(t, plannerConfig, `{} {}`)
			assert.ErrorIs(t, err, config.ErrSpec, "Resolve must refuse a configuration that is not one JSON value")
		})

		t.Run("returns ErrSpec for a requirement that no strategy meets", func(t *testing.T) {
			t.Parallel()
			_, err := config.Resolve(newRegistry(t, newSupport()), workerSpec(),
				map[config.Axis][]config.Capability{config.Store: {config.Durable}})
			assert.ErrorIs(t, err, config.ErrSpec, "Resolve must refuse a namespace without leases")
		})

		t.Run("does not meet a requirement on the membership with a capability of the store", func(t *testing.T) {
			t.Parallel()
			_, err := config.Resolve(newRegistry(t, newSupport()), workerSpec(),
				map[config.Axis][]config.Capability{config.Store: {config.Durable, config.Leases}})
			assert.ErrorIs(t, err, config.ErrSpec, "Resolve must check each requirement against its own role")
		})

		t.Run("returns ErrUnsupported for a capability of the support matrix that the namespace does not have",
			func(t *testing.T) {
				t.Parallel()
				spec := workerSpec()
				spec.Ownership = config.Selection{Name: unfencedName}
				_, err := config.Resolve(newRegistry(t, newSupport()), spec, testProvided)
				expect.ErrorIs(t, err, config.ErrUnsupported, "Resolve must refuse a namespace without fencing")
				expect.Equal(t, errs.Classify(err), errs.Unsupported, "the error must classify as Unsupported")
			})

		t.Run("does not check the capabilities while a selection fails", func(t *testing.T) {
			t.Parallel()
			r := newRegistry(t, newSupport())
			register(t, r, config.Placement, &testFactory[struct{}]{
				name: zoneName,
				parse: func(json.RawMessage) (config.Parsed, error) {
					return nil, errTest
				},
			})
			spec := workerSpec()
			spec.Ownership = config.Selection{Name: unfencedName}
			spec.Placement = config.Selection{Name: zoneName}
			_, err := config.Resolve(r, spec, testProvided)
			expect.ErrorIs(t, err, config.ErrSpec, "Resolve must return the problem of the selection")
			expect.ErrorIsNot(t, err, config.ErrUnsupported,
				"Resolve must not report a capability that it did not check")
		})

		writer := config.Capabilities{Provides: []config.Capability{config.SingleWriter}}
		building := []struct {
			name     string
			axis     config.Axis
			factory  func(r *config.Registry) error
			wantErrs []error
		}{
			{
				name: "returns ErrRegistration for a partitioner factory of another type",
				axis: config.Partitioning,
				factory: func(r *config.Registry) error {
					return config.Register(r, config.Partitioning, &testFactory[struct{}]{name: zoneName})
				},
				wantErrs: []error{config.ErrRegistration},
			},
			{
				name: "returns ErrSpec with the error of New of the partitioner",
				axis: config.Partitioning,
				factory: func(r *config.Registry) error {
					return config.Register(r, config.Partitioning, &testFactory[partition.Partitioner]{
						name: zoneName,
						build: func(config.Parsed) (partition.Partitioner, error) {
							return nil, errTest
						},
					})
				},
				wantErrs: []error{config.ErrSpec, errTest},
			},
			{
				name: "returns ErrSpec for a factory that builds no partitioner",
				axis: config.Partitioning,
				factory: func(r *config.Registry) error {
					return config.Register(r, config.Partitioning, &testFactory[partition.Partitioner]{name: zoneName})
				},
				wantErrs: []error{config.ErrSpec},
			},
			{
				name: "returns ErrRegistration for a model factory of another type",
				axis: config.Replication,
				factory: func(r *config.Registry) error {
					return config.Register(r, config.Replication, &testFactory[struct{}]{name: zoneName, caps: writer})
				},
				wantErrs: []error{config.ErrRegistration},
			},
			{
				name: "returns ErrSpec with the error of New of the model",
				axis: config.Replication,
				factory: func(r *config.Registry) error {
					return config.Register(r, config.Replication, &testFactory[*replication.Compiled]{
						name: zoneName,
						caps: writer,
						build: func(config.Parsed) (*replication.Compiled, error) {
							return nil, errTest
						},
					})
				},
				wantErrs: []error{config.ErrSpec, errTest},
			},
			{
				name: "returns ErrSpec for a factory that builds no model",
				axis: config.Replication,
				factory: func(r *config.Registry) error {
					return config.Register(r, config.Replication, &testFactory[*replication.Compiled]{
						name: zoneName,
						caps: writer,
					})
				},
				wantErrs: []error{config.ErrSpec},
			},
		}
		for _, tt := range building {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				s := newSupport()
				s.Strategies[tt.axis] = append(s.Strategies[tt.axis], zoneName)
				r := newRegistry(t, s)
				assert.NoError(t, tt.factory(r), "the factory of the case must register")
				spec := workerSpec()
				if tt.axis == config.Partitioning {
					spec.Partitioning.Name = zoneName
				} else {
					spec.Replication.Name = zoneName
				}
				_, err := config.Resolve(r, spec, testProvided)
				for _, want := range tt.wantErrs {
					expect.ErrorIs(t, err, want, "Resolve must return the error of the build")
				}
				expect.Equal(t, errs.Classify(err), errs.Invalid, "the error must classify as Invalid")
			})
		}

		t.Run("returns ErrSpec for a drain role that the model does not declare", func(t *testing.T) {
			t.Parallel()
			spec := workerSpec()
			spec.Budget.DrainRoles = []shard.Role{shard.RolePrimary, shard.RoleLeader}
			_, err := config.Resolve(newRegistry(t, newSupport()), spec, testProvided)
			assert.ErrorIs(t, err, config.ErrSpec, "Resolve must refuse a drain role outside the model")
		})

		t.Run("returns ErrSpec for a drain role of the preset that the model does not declare", func(t *testing.T) {
			t.Parallel()
			r := newRegistry(t, newSupport())
			p := newPreset()
			p.Name = tunedPreset
			p.Spec.Budget.DrainRoles = []shard.Role{shard.RolePrimary, shard.RoleLeader}
			assert.NoError(t, config.RegisterPreset(r, p), "the preset must register")
			_, err := config.Resolve(r, &config.Spec{Namespace: testSpace, Preset: tunedPreset}, testProvided)
			assert.ErrorIs(t, err, config.ErrSpec, "Resolve must check the drain roles of the preset against the model")
		})

		t.Run("reports every problem of one class in one error", func(t *testing.T) {
			t.Parallel()
			spec := workerSpec()
			spec.Load.Window = -1
			spec.Cells.MaxNodes = -1
			_, err := config.Resolve(newRegistry(t, newSupport()), spec, testProvided)
			assert.ErrorIs(t, err, config.ErrSpec, "Resolve must refuse the spec")
			expect.Contains(t, err.Error(), placeWindow, "the error must contain the problem of the window")
			expect.Contains(t, err.Error(), placeMaxNodes, "the error must contain the problem of the cell bound")
			expect.Equal(t, errs.Classify(err), errs.Invalid, "the error must classify as Invalid")
		})

		t.Run("returns a resolved spec that shares no slices with the spec", func(t *testing.T) {
			t.Parallel()
			spec := workerSpec()
			spec.Budget.DrainRoles = []shard.Role{shard.RolePrimary}
			spec.Load.Metrics = []config.Metric{{Name: cpuMetric}}
			res, err := config.Resolve(newRegistry(t, newSupport()), spec, testProvided)
			assert.NoError(t, err, "the spec must resolve")
			spec.Budget.DrainRoles[0] = shard.RoleLeader
			spec.Load.Metrics[0].Name = bytesMetric
			expect.Equal(t, res.Spec.Budget.DrainRoles, []shard.Role{shard.RolePrimary},
				"a change of the drain roles of the spec must not change the result")
			expect.Equal(t, res.Spec.Load.Metrics, []config.Metric{{Name: cpuMetric}},
				"a change of the metrics of the spec must not change the result")
		})

		t.Run("returns a resolved spec that shares no slices with the preset", func(t *testing.T) {
			t.Parallel()
			r := newRegistry(t, newSupport())
			p := newPreset()
			p.Name = tunedPreset
			p.Spec.Budget.DrainRoles = []shard.Role{shard.RolePrimary}
			p.Spec.Load.Metrics = []config.Metric{{Name: cpuMetric}}
			assert.NoError(t, config.RegisterPreset(r, p), "the preset must register")
			res, err := config.Resolve(r, &config.Spec{Namespace: testSpace, Preset: tunedPreset}, testProvided)
			assert.NoError(t, err, "the spec must resolve")
			p.Spec.Budget.DrainRoles[0] = shard.RoleLeader
			p.Spec.Load.Metrics[0].Name = bytesMetric
			expect.Equal(t, res.Spec.Budget.DrainRoles, []shard.Role{shard.RolePrimary},
				"a change of the drain roles of the preset must not change the result")
			expect.Equal(t, res.Spec.Load.Metrics, []config.Metric{{Name: cpuMetric}},
				"a change of the metrics of the preset must not change the result")
		})

		t.Run("calls the factories without the lock of the registry", func(t *testing.T) {
			t.Parallel()
			r := newRegistry(t, newSupport())
			register(t, r, config.Placement, &testFactory[struct{}]{
				name: zoneName,
				parse: func(raw json.RawMessage) (config.Parsed, error) {
					registered := make(chan error)
					go func() {
						registered <- config.Register(r, config.Goal, &testFactory[struct{}]{name: zoneName})
					}()
					return rawConfig(emptyConfig), <-registered
				},
			})
			spec := workerSpec()
			spec.Placement = config.Selection{Name: zoneName}
			var err error
			assert.CompletesWithin(t, 5*time.Second, func(context.Context) error {
				_, err = config.Resolve(r, spec, testProvided)
				return err
			}, "Resolve must call the factories without the lock of the registry")
			assert.NoError(t, err, "the factory must register while Resolve parses")
		})
	})
}

// resolveMerge resolves the patch over the target with Resolve. It registers a preset
// whose placement configuration is target, and resolves a spec over that preset whose
// placement configuration is patch.
func resolveMerge(t *testing.T, target, patch string) (*config.Resolved, error) {
	t.Helper()
	r := newRegistry(t, newSupport())
	p := newPreset()
	p.Name = mergePreset
	p.Spec.Placement.Config = json.RawMessage(target)
	assert.NoError(t, config.RegisterPreset(r, p), "the preset must register")
	spec := &config.Spec{
		Namespace: testSpace,
		Preset:    mergePreset,
		Placement: config.Selection{Config: json.RawMessage(patch)},
	}
	return config.Resolve(r, spec, testProvided)
}
