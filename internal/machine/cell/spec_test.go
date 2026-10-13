// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package cell_test

import (
	"encoding/json"
	"errors"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/control"
	"go.thesmos.sh/sharder/internal/follow"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/partition/singleton"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// unknownPreset is a spec with a preset that the registry does not have.
const unknownPreset = `{"namespace":"orders","preset":"unknown","version":1}`

// The names of the strategies of the tests.
const (
	// otherHash is the name of a hash function that the registry does not have.
	otherHash shard.StrategyName = "fnv1a"
	// staticName is the name of a placer that is not the planner.
	staticName shard.StrategyName = "static"
)

// logSpec is the log message of a newer spec that does not apply.
const logSpec = "cell: the accepted spec does not apply"

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

// TestSpec checks the specs that a cell resolves from the root scope and passes to its
// controller.
func TestSpec(t *testing.T) {
	t.Parallel()

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			give option
			name string
		}{
			{
				name: "stops with control.ErrSpec for a root spec that does not decode",
				give: func(s *setup) { s.root.Spec = []byte(malformed) },
			},
			{
				name: "stops with control.ErrSpec for a root spec that does not resolve",
				give: func(s *setup) { s.root.Spec = []byte(unknownPreset) },
			},
			{
				name: "stops with control.ErrSpec for a root that recorded another partitioner",
				give: func(s *setup) { s.remap = func(m *shard.Mapping) { m.Partitioner = singleton.Name } },
			},
			{
				name: "stops with control.ErrSpec for a root that recorded another hash function",
				give: func(s *setup) { s.remap = func(m *shard.Mapping) { m.Hash = otherHash } },
			},
			{
				name: "stops with control.ErrSpec for a root that recorded another version of the hash function",
				give: func(s *setup) { s.remap = func(m *shard.Mapping) { m.HashVersion++ } },
			},
			{
				name: "stops with control.ErrSpec for a root that recorded another configuration of the partitioner",
				give: func(s *setup) { s.remap = func(m *shard.Mapping) { m.Config = []byte(otherSlots) } },
			},
			{
				name: "stops with control.ErrSpec for a spec whose strategies do not build",
				give: func(s *setup) {
					s.support.Strategies[config.Placement] = append(s.support.Strategies[config.Placement], staticName)
					s.extend = func(r *config.Registry) error {
						return config.Register[placement.Placer](r, config.Placement, static{})
					}
					s.spec.Placement = config.Selection{Name: staticName}
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				h := newHarness(t, tt.give)
				h.start(t)
				assert.ErrorIs(t, h.machine.Err(), control.ErrSpec, "the cell must stop at a spec that does not apply")
			})
		}

		t.Run("passes a newer spec to its controller", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.view(t, nodeA, nodeB)
			h.commitSpec(t, firstVersion+1, nil)
			assert.Equal(t, h.state(t).Cell.SpecVersion, firstVersion+1,
				"the controller must record the version of the newer spec")
		})

		t.Run("logs a newer spec that does not apply", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.view(t, nodeA, nodeB)
			h.commitSpec(t, firstVersion+1, func(s *config.Spec) {
				s.Partitioning = config.Selection{Name: fixedslots.Name, Config: json.RawMessage(otherSlots)}
			})
			got := h.logged(logSpec)
			assert.Length(t, got, 1, "the cell must log the spec that does not apply")
			expect.Equal(t, got[0][attrCellKey], string(cellID), "the log must name the cell")
			expect.Equal(t, h.state(t).Cell.SpecVersion, firstVersion, "the cell must keep its spec")
		})

		t.Run("passes the spec of a new snapshot of the root scope to its controller", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.view(t, nodeA, nodeB)
			h.commitRoot(t, specMutation(t, firstVersion+1, nil))
			h.commitRoot(t, &store.Mutation{})
			h.st.Compact()
			h.follow(t)
			h.elapse(t, follow.RetryBase/2)
			assert.Equal(t, h.state(t).Cell.SpecVersion, firstVersion+1,
				"the controller must adopt the spec of the snapshot")
		})

		t.Run("ignores a generation of the root scope without a spec", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.view(t, nodeA, nodeB)
			h.commitRoot(t, &store.Mutation{})
			h.follow(t)
			expect.Length(t, h.plans, 1, "a generation without a spec must not start a pass")
			expect.Empty(t, h.logged(logSpec), "a generation without a spec must not resolve a spec")
		})

		t.Run("reads a root scope of many pages", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) {
				s.root.Put = append(s.root.Put, otherCells(pageLimit)...)
			})
			h.start(t)
			assert.Nil(t, h.machine.Err(), "the cell must serve after a snapshot of the root of many pages")
			assert.NotEqual(t, h.views, machine.Token(0), "the cell must watch its views after the snapshot")
		})
	})
}
