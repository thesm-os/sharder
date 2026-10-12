// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/handoff"
	"go.thesmos.sh/sharder/internal/machine/agent"
	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// The specs of the tests.
const (
	// malformed is text that is not JSON.
	malformed = "{"
	// unknownPreset is a spec with a preset that the registry does not have.
	unknownPreset = `{"namespace":"orders","preset":"unknown","version":1}`
	// timeouts is a configuration of break-before-make with other timeouts than the default.
	timeouts = `{"addTimeout":"7s","dropTimeout":"3s"}`
)

// The timeouts and the window of the configurations of the tests.
const (
	otherAdd    = 7 * time.Second
	otherDrop   = 3 * time.Second
	otherWindow = 2 * time.Minute
)

// staticName is the name of the strategies of the tests that are not strategies of the
// support matrix.
const staticName shard.StrategyName = "static"

// errStatic is the error of the factories of the strategies of the tests.
var errStatic = errors.New("test: the strategy does not build")

// none is the configuration of a strategy of the tests without settings.
type none struct{}

// Validate returns nil.
func (none) Validate() error { return nil }

// staticProtocol is the factory of a handoff protocol other than break-before-make. It
// provides the capability of exclusive ownership, so a spec with it resolves.
type staticProtocol struct{}

var _ config.Factory[handoff.Protocol] = staticProtocol{}

// Name returns staticName.
func (staticProtocol) Name() shard.StrategyName { return staticName }

// Parse returns the configuration without settings.
func (staticProtocol) Parse(json.RawMessage) (config.Parsed, error) { return none{}, nil }

// Capabilities provides the capability exclusive-safe.
func (staticProtocol) Capabilities(config.Parsed) config.Capabilities {
	return config.Capabilities{Provides: []config.Capability{config.ExclusiveSafe}}
}

// New returns errStatic.
func (staticProtocol) New(config.Parsed) (handoff.Protocol, error) { return nil, errStatic }

// staticOwnership is the factory of an ownership mode whose New fails. It provides the
// capability fenced, so a spec with it resolves.
type staticOwnership struct{}

var _ config.Factory[ownership.Config] = staticOwnership{}

// Name returns staticName.
func (staticOwnership) Name() shard.StrategyName { return staticName }

// Parse returns the configuration without settings.
func (staticOwnership) Parse(json.RawMessage) (config.Parsed, error) { return none{}, nil }

// Capabilities provides the capability fenced.
func (staticOwnership) Capabilities(config.Parsed) config.Capabilities {
	return config.Capabilities{Provides: []config.Capability{config.Fenced}}
}

// New returns errStatic.
func (staticOwnership) New(config.Parsed) (ownership.Config, error) {
	return ownership.Config{}, errStatic
}

// TestSpec checks the parts of the spec of the namespace that an agent applies.
func TestSpec(t *testing.T) {
	t.Parallel()

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("stops with the error of the decoder for a spec that does not decode", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.root.Spec = []byte(malformed) })
			h.start(t)
			err := h.machine.Err()
			expect.ErrorIs(t, err, agent.ErrConfig, "the agent must stop at a spec that it cannot apply")
			_ = expect.ErrorAs[*json.SyntaxError](t, err, "the error must wrap the error of the decoder")
		})

		tests := []struct {
			give option
			name string
		}{
			{
				name: "stops with ErrConfig for a spec that does not resolve",
				give: func(s *setup) { s.root.Spec = []byte(unknownPreset) },
			},
			{
				name: "stops with ErrConfig for a handoff protocol other than break-before-make",
				give: func(s *setup) {
					s.support.Strategies[config.Handoff] = append(s.support.Strategies[config.Handoff], staticName)
					s.extend = func(r *config.Registry) error {
						return config.Register[handoff.Protocol](r, config.Handoff, staticProtocol{})
					}
					s.spec.Handoff = config.Selection{Name: staticName}
				},
			},
			{
				name: "stops with ErrConfig for an ownership mode that does not build",
				give: func(s *setup) {
					s.support.Strategies[config.Ownership] = append(s.support.Strategies[config.Ownership], staticName)
					s.extend = func(r *config.Registry) error {
						return config.Register[ownership.Config](r, config.Ownership, staticOwnership{})
					}
					s.spec.Ownership = config.Selection{Name: staticName}
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				h := newHarness(t, tt.give)
				h.start(t)
				assert.ErrorIs(
					t,
					h.machine.Err(),
					agent.ErrConfig,
					"the agent must stop at a spec that it cannot apply",
				)
			})
		}

		t.Run("times an add and a drop with the timeouts of break-before-make", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.spec.Handoff = config.Selection{Config: json.RawMessage(timeouts)} })
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.do(t, h.request(protocol.Drop, shardA, 2, firstEpoch))
			assert.Length(t, h.handles, 2, "the agent must call the handler for the add and the drop")
			expect.Equal(t, h.acquires[0].Timeout, otherAdd, "the acquisition must have the timeout of an add")
			expect.Equal(t, h.handles[0].Timeout, otherAdd, "the add must have the timeout of an add")
			expect.Equal(t, h.handles[1].Timeout, otherDrop, "the drop must have the timeout of a drop")
		})

		t.Run("reports again after the load window of the spec", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) {
				s.spec.Load = config.LoadModel{Window: shard.Duration(otherWindow)}
			})
			h.start(t)
			h.elapse(t, otherWindow)
			assert.Length(t, h.reports, 2, "the agent must report again after the load window of the spec")
		})
	})
}
