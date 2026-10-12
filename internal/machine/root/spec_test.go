// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package root_test

import (
	"encoding/json"
	"errors"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/telemetry"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/control"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/root"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// The names of the strategies of the tests.
const (
	// staticName is the name of a placer that is not the planner.
	staticName shard.StrategyName = "static"
	// unknownPreset is a preset that the registry does not have.
	unknownPreset config.PresetName = "unknown"
)

// errStatic is the error of the factory of the placer static.
var errStatic = errors.New("test: the placer does not build")

// none is the configuration of a strategy of the tests without settings.
type none struct{}

// Validate returns nil.
func (none) Validate() error { return nil }

// static is the factory of a placer other than the planner. A controller refuses such a
// placer before it builds one, so its New returns an error.
type static struct{}

var _ config.Factory[placement.Placer] = static{}

// Name returns staticName.
func (static) Name() shard.StrategyName { return staticName }

// Parse returns the configuration without settings.
func (static) Parse(json.RawMessage) (config.Parsed, error) { return none{}, nil }

// Capabilities returns no capabilities.
func (static) Capabilities(config.Parsed) config.Capabilities { return config.Capabilities{} }

// New returns errStatic.
func (static) New(config.Parsed) (placement.Placer, error) { return nil, errStatic }

// TestSpec checks the namespace that the root creates, the strategies with which it places
// the cell, and the spec proposals that it applies.
func TestSpec(t *testing.T) {
	t.Parallel()

	t.Run("New", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrConfig for a spec that does not resolve", func(t *testing.T) {
			t.Parallel()
			cfg := completeConfig(t)
			cfg.Spec.Preset = unknownPreset
			_, err := root.New(&cfg)
			assert.ErrorIs(t, err, root.ErrConfig, "New must refuse a spec that does not resolve")
		})

		t.Run("returns ErrConfig for a spec whose strategies do not build", func(t *testing.T) {
			t.Parallel()
			cfg := completeConfig(t)
			support := builtin.Support()
			support.Strategies[config.Placement] = append(support.Strategies[config.Placement], staticName)
			cfg.Registry = config.NewRegistry(support)
			assert.NoError(t, builtin.Register(cfg.Registry), "the built-ins must register")
			assert.NoError(t, config.Register[placement.Placer](cfg.Registry, config.Placement, static{}),
				"the placer of the test must register")
			cfg.Spec.Placement = config.Selection{Name: staticName}
			_, err := root.New(&cfg)
			expect.That(t, err).
				ErrorIs(root.ErrConfig, "New must refuse a spec whose strategies do not build").
				ErrorIs(control.ErrSpec, "the error must wrap the error of the build")
		})

		t.Run("returns ErrConfig for a registry without the strategies of the root", func(t *testing.T) {
			t.Parallel()
			cfg := completeConfig(t)
			cfg.Registry = config.NewRegistry(builtin.Support())
			for _, err := range []error{
				config.Register(cfg.Registry, config.Partitioning, builtin.FixedSlots(cfg.Registry)),
				config.Register(cfg.Registry, config.Hash, builtin.XXHash64()),
				config.Register(cfg.Registry, config.Placement, builtin.Planner(cfg.Registry)),
				config.Register(cfg.Registry, config.Goal, builtin.Count()),
				config.Register(cfg.Registry, config.Replication, builtin.PrimaryOnly()),
				config.Register(cfg.Registry, config.Ownership, builtin.Exclusive()),
				config.Register(cfg.Registry, config.Handoff, builtin.BreakBeforeMake()),
				config.Register(cfg.Registry, config.Topology, builtin.Embedded()),
				config.Register(cfg.Registry, config.Routing, builtin.Watch()),
			} {
				assert.NoError(t, err, "a built-in must register")
			}
			presets := builtin.Presets()
			for i := range presets {
				assert.NoError(t, config.RegisterPreset(cfg.Registry, &presets[i]), "a preset must register")
			}
			_, err := root.New(&cfg)
			assert.ErrorIs(t, err, root.ErrConfig, "New must refuse a registry without the strategies of the root")
		})
	})

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("creates the namespace in a root scope without a commit", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			st := h.state(t)
			want := namespaceSpec()
			want.Version = 1
			res, err := config.Resolve(registry(t), &want, provided)
			assert.NoError(t, err, "the spec of the tests must resolve")
			raw, err := json.Marshal(&res.Spec)
			assert.NoError(t, err, "the resolved spec must encode")
			expect.False(t, st.Head.Incarnation.IsZero(), "the root must create the namespace in an incarnation")
			expect.Equal(t, st.Mapping, res.Mapping, "the root must record the mapping of the spec")
			expect.Equal(t, string(st.Spec), string(raw), "the root must record the spec with every default")
		})

		t.Run("acquires an existing namespace in its incarnation", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.create(t, 3)
			before := h.state(t).Spec
			h.start(t)
			st := h.state(t)
			expect.Equal(t, st.Head.Incarnation, incB, "the root must acquire the namespace in its incarnation")
			expect.Equal(t, st.Spec, before, "the root must keep the spec of an existing namespace")
		})

		t.Run("takes the namespace that another root creates first", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.step(t, machine.Started{Reply: h.call()})
			h.deliver(t)
			h.deliver(t)
			h.create(t, 3)
			h.run(t)
			expect.Equal(t, h.state(t).Head.Incarnation, incB, "the root must take the incarnation of the other root")
			expect.Equal(t, h.state(t).Cell.Controller, rootAddress, "the root must commit its record")
		})

		t.Run("places the cell of the namespace on a controller process", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.view(t, procA)
			ps := h.placements(t)
			assert.Length(t, ps, 1, "the root must place one cell")
			expect.Equal(t, ps[0].Range, shard.Range{}, "the cell must cover every position")
			expect.Equal(t, ps[0].Replicas, []shard.Replica{{Node: procA.ID, Role: shard.RolePrimary}},
				"the controller of the cell must run on the controller process")
			assert.NotEmpty(t, h.sent, "the root must send the add of the cell")
			add := h.sent[len(h.sent)-1]
			expect.Equal(t, add.Call, protocol.Add, "the root must add the cell to the process")
			expect.Equal(t, add.Scope, rootScope, "the callback must have the root scope")
			expect.Equal(t, add.Shard, ps[0].Shard, "the callback must name the shard of the cell")
		})

		t.Run("places the cell on a controller process with a capacity for it", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			full := procA
			full.Capacity = map[string]float64{root.Capacity: 0}
			h.view(t, full, procB)
			ps := h.placements(t)
			assert.Length(t, ps, 1, "the root must place one cell")
			assert.Equal(t, ps[0].Replicas, []shard.Replica{{Node: procB.ID, Role: shard.RolePrimary}},
				"the root must not place a cell on a process without a capacity for it")
		})

		t.Run("moves the cell off a controller process that leaves", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.view(t, procA)
			h.view(t, procB)
			ps := h.placements(t)
			assert.Length(t, ps, 1, "the root must keep one cell")
			assert.Equal(t, ps[0].Replicas, []shard.Replica{{Node: procB.ID, Role: shard.RolePrimary}},
				"the root must move the cell to the process that remains")
		})

		t.Run("records the writer gap of a cell at the commit that ends its move", func(t *testing.T) {
			t.Parallel()
			r := &histograms{Reporter: noop.New(), records: make(map[telemetry.InstrumentName]int)}
			h := newHarness(t, func(s *setup) { s.cfg.Reporter = r })
			h.start(t)
			h.view(t, procA)
			h.view(t, procB)
			assert.Equal(t, r.records[gapInstrument], 1, "the root must record the gap of the cell when its move ends")
		})

		t.Run("applies a spec proposal", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			call := h.submitCall(t, h.specProposal(t, 1, 1, 2), operator)
			h.step(t, call)
			h.run(t)
			got := replyTo[protocol.Receipt](t, h, call.Reply)
			assert.NoError(t, got.Err, "the root must apply the spec")
			expect.True(t, got.Value.Applied, "the receipt must report that the root applied the spec")
			var accepted config.Spec
			assert.NoError(t, json.Unmarshal(h.state(t).Spec, &accepted), "the root scope must contain a spec")
			expect.Equal(t, accepted.Version, epoch.Epoch(2), "the root scope must accept the spec of the proposal")
		})

		t.Run("returns control.ErrCall for a proposal of another kind", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			p := h.specProposal(t, 1, 1, 2)
			p.Kind = proposal.Drain
			call := h.submitCall(t, p, operator)
			h.step(t, call)
			assert.ErrorIs(t, replyTo[protocol.Receipt](t, h, call.Reply).Err, control.ErrCall,
				"the root must refuse a proposal that is not a spec")
		})

		t.Run("returns control.ErrDenied for a proposal of a caller that is not an operator", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			call := h.submitCall(t, h.specProposal(t, 1, 1, 2), procA.Identity)
			h.step(t, call)
			assert.ErrorIs(t, replyTo[protocol.Receipt](t, h, call.Reply).Err, control.ErrDenied,
				"the root must authenticate the callers of its config")
		})
	})
}
