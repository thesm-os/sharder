// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin_test

import (
	"encoding/json"
	"testing"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/partition/singleton"
	"go.thesmos.sh/sharder/partition/xxhash64"
	"go.thesmos.sh/sharder/shard"
)

// The hash that a test registers as a hash of an application.
const (
	// otherHash is the name under which a test registers xxhash64 a second time.
	otherHash shard.StrategyName = "xxhash64-copy"
	// otherHashConfig is the configuration of fixed-slots that selects otherHash.
	otherHashConfig = `{"hash":"` + string(otherHash) + `"}`
)

// TestPartitioning checks the factories of the partitioners fixed-slots and singleton.
func TestPartitioning(t *testing.T) {
	t.Parallel()

	t.Run("FixedSlots", func(t *testing.T) {
		t.Parallel()

		t.Run("Parse", func(t *testing.T) {
			t.Parallel()

			t.Run("returns 1024 slots of xxhash64 without hash tags for no configuration", func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, parse(t, builtin.FixedSlots(newRegistry(t)), ""),
					config.Parsed(&fixedslots.Config{Hash: xxhash64.Name, Slots: 1024}),
					"Parse must return the defaults of fixed-slots")
			})

			t.Run("keeps the slots and the hash tags that the configuration sets", func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, parse(t, builtin.FixedSlots(newRegistry(t)), `{"slots":16,"hashTags":true}`),
					config.Parsed(&fixedslots.Config{Hash: xxhash64.Name, Slots: 16, HashTags: true}),
					"Parse must keep the fields of the configuration")
			})

			t.Run("returns ErrConfig for a hash that the registry does not have", func(t *testing.T) {
				t.Parallel()
				_, err := builtin.FixedSlots(newRegistry(t)).Parse(json.RawMessage(otherHashConfig))
				assert.ErrorIs(t, err, builtin.ErrConfig, "Parse must refuse a hash without a factory")
				assert.ErrorIs(t, err, config.ErrUnknown, "Parse must wrap the error of the registry")
			})

			t.Run("returns the configuration of its own encoding", func(t *testing.T) {
				t.Parallel()
				f := builtin.FixedSlots(newRegistry(t))
				roundTrip(t, f, parse(t, f, `{"slots":16,"hashTags":true}`))
			})
		})

		t.Run("Capabilities", func(t *testing.T) {
			t.Parallel()

			t.Run("returns no capabilities", func(t *testing.T) {
				t.Parallel()
				f := builtin.FixedSlots(newRegistry(t))
				assert.Equal(t, f.Capabilities(parse(t, f, "")), config.Capabilities{},
					"fixed-slots must not provide or require a capability")
			})
		})

		t.Run("New", func(t *testing.T) {
			t.Parallel()

			t.Run("builds fixed-slots with the hash of the configuration", func(t *testing.T) {
				t.Parallel()
				f := builtin.FixedSlots(newRegistry(t))
				got, err := f.New(parse(t, f, `{"slots":16}`))
				assert.NoError(t, err, "New must build fixed-slots")
				want, err := fixedslots.New(fixedslots.Config{Hash: xxhash64.Name, Slots: 16}, xxhash64.Hash{})
				assert.NoError(t, err, "fixedslots.New must build the partitioner of the test")
				assert.Equal(t, got, partition.Partitioner(want), "New must build the partitioner of its configuration")
			})

			t.Run("returns ErrConfig for a hash that the registry cannot build", func(t *testing.T) {
				t.Parallel()
				_, err := builtin.FixedSlots(config.NewRegistry(builtin.Support())).New(
					&fixedslots.Config{Hash: xxhash64.Name, Slots: 16})
				assert.ErrorIs(t, err, builtin.ErrConfig, "New must refuse a hash without a factory")
				assert.ErrorIs(t, err, config.ErrUnknown, "New must wrap the error of the registry")
			})

			t.Run("returns ErrConfig for a configuration that fixedslots.New refuses", func(t *testing.T) {
				t.Parallel()
				_, err := builtin.FixedSlots(newRegistry(t)).New(&fixedslots.Config{Hash: xxhash64.Name})
				assert.ErrorIs(t, err, builtin.ErrConfig, "New must refuse a configuration without slots")
				assert.ErrorIs(t, err, fixedslots.ErrConfig, "New must wrap the error of fixedslots.New")
			})

			changes := []struct {
				name string
				give string
			}{
				{name: "builds a mapping that a change of the slots changes", give: `{"slots":2048}`},
				{name: "builds a mapping that a change of the hash changes", give: otherHashConfig},
				{name: "builds a mapping that a change of the hash tags changes", give: `{"hashTags":true}`},
			}
			for _, tt := range changes {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					r := newRegistry(t)
					hash := renamed[partition.Hash](t, builtin.XXHash64(), otherHash)
					assert.NoError(t, config.Register(r, config.Hash, hash),
						"the registry must take the hash of the application")
					from, err := config.Resolve(r, &config.Spec{Namespace: testSpace, Preset: builtin.ExclusiveWorker},
						provided)
					assert.NoError(t, err, "the preset must resolve")
					to, err := config.Resolve(r, &config.Spec{
						Namespace:    testSpace,
						Preset:       builtin.ExclusiveWorker,
						Partitioning: config.Selection{Config: json.RawMessage(tt.give)},
					}, provided)
					assert.NoError(t, err, "the changed spec must resolve")
					assert.ErrorIs(t, config.Change(from, to), config.ErrChange,
						"Change must refuse an edit of the mapping")
				})
			}
		})
	})

	t.Run("Singleton", func(t *testing.T) {
		t.Parallel()

		t.Run("Parse", func(t *testing.T) {
			t.Parallel()

			t.Run("returns ErrConfig for a field", func(t *testing.T) {
				t.Parallel()
				_, err := builtin.Singleton().Parse(json.RawMessage(`{"slots":16}`))
				assert.ErrorIs(t, err, builtin.ErrConfig, "Parse must refuse every field of the singleton")
			})
		})

		t.Run("Capabilities", func(t *testing.T) {
			t.Parallel()

			t.Run("returns no capabilities", func(t *testing.T) {
				t.Parallel()
				f := builtin.Singleton()
				assert.Equal(t, f.Capabilities(parse(t, f, "")), config.Capabilities{},
					"the singleton must not provide or require a capability")
			})
		})

		t.Run("New", func(t *testing.T) {
			t.Parallel()

			t.Run("builds the singleton", func(t *testing.T) {
				t.Parallel()
				f := builtin.Singleton()
				got, err := f.New(parse(t, f, ""))
				assert.NoError(t, err, "New must build the singleton")
				assert.Equal(t, got, partition.Partitioner(singleton.Partitioner{}), "New must return the singleton")
			})
		})
	})
}
