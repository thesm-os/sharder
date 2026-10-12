// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin_test

import (
	"encoding/json"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/partition/singleton"
	"go.thesmos.sh/sharder/partition/xxhash64"
	"go.thesmos.sh/sharder/placement/goal"
	"go.thesmos.sh/sharder/placement/planner"
	"go.thesmos.sh/sharder/shard"
)

// emptyJSON is the encoding of a configuration without fields.
const emptyJSON = `{}`

// TestPreset checks the presets of the support matrix.
func TestPreset(t *testing.T) {
	t.Parallel()

	t.Run("Presets", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the four presets of the support matrix", func(t *testing.T) {
			t.Parallel()
			presets := builtin.Presets()
			names := make([]config.PresetName, len(presets))
			for i, p := range presets {
				names[i] = p.Name
			}
			assert.Equal(t, names, []config.PresetName{
				builtin.ExclusiveWorker, builtin.ConsumerGroup, builtin.ActorEntities, builtin.LeaderElection,
			}, "Presets must return the presets of the matrix in their order")
		})

		count := planner.GoalSelection{Name: goal.CountName, Config: json.RawMessage(emptyJSON), Weight: 1}
		stickiness := planner.GoalSelection{
			Name: goal.StickinessName, Config: json.RawMessage(emptyJSON), Tier: 1, Weight: 1,
		}
		slots := &fixedslots.Config{Hash: xxhash64.Name, Slots: 1024}
		tests := []struct {
			name      string
			preset    config.PresetName
			partition shard.StrategyName
			wantSlots *fixedslots.Config
			wantGoals []planner.GoalSelection
		}{
			{
				name:      "returns exclusive-worker with 1024 slots and the goal count",
				preset:    builtin.ExclusiveWorker,
				partition: fixedslots.Name,
				wantSlots: slots,
				wantGoals: []planner.GoalSelection{count},
			},
			{
				name:      "returns consumer-group with the default slots, count and stickiness",
				preset:    builtin.ConsumerGroup,
				partition: fixedslots.Name,
				wantSlots: slots,
				wantGoals: []planner.GoalSelection{count, stickiness},
			},
			{
				name:      "returns actor-entities with the default slots and the goal count",
				preset:    builtin.ActorEntities,
				partition: fixedslots.Name,
				wantSlots: slots,
				wantGoals: []planner.GoalSelection{count},
			},
			{
				name:      "returns leader-election with the singleton and no goals",
				preset:    builtin.LeaderElection,
				partition: singleton.Name,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				res, err := config.Resolve(newRegistry(t), &config.Spec{Namespace: testSpace, Preset: tt.preset},
					provided)
				assert.NoError(t, err, "the preset must resolve without a problem")
				expect.Equal(t, res.Spec.Partitioning.Name, tt.partition, "the preset must select its partitioner")
				if tt.wantSlots != nil {
					expect.Equal(t, res.Configs[config.Partitioning], config.Parsed(tt.wantSlots),
						"the preset must select its slots")
				}
				expect.Equal(t, res.Configs[config.Placement].(*planner.Config).Goals, tt.wantGoals,
					"the preset must select its goals")
				expect.True(t, res.Configs[config.Ownership].(*ownership.Config).Fenced,
					"the preset must select a fenced exclusive ownership")
			})
		}

		t.Run("returns new presets on each call", func(t *testing.T) {
			t.Parallel()
			want := builtin.Presets()
			got := builtin.Presets()
			got[0].Spec.Ownership.Config[0] = ' '
			got[1].Name = builtin.LeaderElection
			assert.Equal(t, builtin.Presets(), want, "a change of a preset must not change the next presets")
		})
	})
}
