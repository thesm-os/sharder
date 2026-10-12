// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin_test

import (
	"encoding/json"
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/handoff/breakbeforemake"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/partition/singleton"
	"go.thesmos.sh/sharder/partition/xxhash64"
	"go.thesmos.sh/sharder/placement/constraint"
	"go.thesmos.sh/sharder/placement/goal"
	"go.thesmos.sh/sharder/placement/planner"
	"go.thesmos.sh/sharder/replication"
	"go.thesmos.sh/sharder/shard"
)

// TestFactory checks the parsing and the building that every factory of the package
// shares, through the factories of the constraints isolation and capacity, of the
// partitioner singleton and of exclusive ownership.
func TestFactory(t *testing.T) {
	t.Parallel()

	t.Run("Name", func(t *testing.T) {
		t.Parallel()

		r := config.NewRegistry(builtin.Support())
		tests := []struct {
			factory interface{ Name() shard.StrategyName }
			want    shard.StrategyName
		}{
			{factory: builtin.FixedSlots(r), want: fixedslots.Name},
			{factory: builtin.Singleton(), want: singleton.Name},
			{factory: builtin.XXHash64(), want: xxhash64.Name},
			{factory: builtin.Planner(r), want: planner.Name},
			{factory: builtin.Capacity(), want: constraint.CapacityName},
			{factory: builtin.Drain(), want: constraint.DrainName},
			{factory: builtin.Isolation(), want: constraint.IsolationName},
			{factory: builtin.Labels(), want: constraint.LabelsName},
			{factory: builtin.MatchLabel(), want: constraint.MatchLabelName},
			{factory: builtin.MaxPerNode(), want: constraint.MaxPerNodeName},
			{factory: builtin.Balance(), want: goal.BalanceName},
			{factory: builtin.Colocate(), want: goal.ColocateName},
			{factory: builtin.Count(), want: goal.CountName},
			{factory: builtin.Prefer(), want: goal.PreferName},
			{factory: builtin.Spread(), want: goal.SpreadName},
			{factory: builtin.Stickiness(), want: goal.StickinessName},
			{factory: builtin.Utilisation(), want: goal.UtilisationName},
			{factory: builtin.PrimaryOnly(), want: replication.PrimaryOnlyName},
			{factory: builtin.Exclusive(), want: builtin.ExclusiveName},
			{factory: builtin.BreakBeforeMake(), want: breakbeforemake.Name},
			{factory: builtin.Embedded(), want: builtin.EmbeddedName},
			{factory: builtin.Watch(), want: builtin.WatchName},
		}
		for _, tt := range tests {
			t.Run("returns "+string(tt.want), func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, tt.factory.Name(), tt.want, "Name must return the registered name of the strategy")
			})
		}
	})

	t.Run("Parse", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the defaults for the JSON null", func(t *testing.T) {
			t.Parallel()
			got, err := builtin.Isolation().Parse(json.RawMessage(`null`))
			assert.NoError(t, err, "Parse must accept the JSON null")
			assert.Equal(t, got, config.Parsed(&constraint.IsolationConfig{Max: 1}),
				"Parse must return the defaults for the JSON null")
		})

		t.Run("keeps the fields of the configuration over the defaults", func(t *testing.T) {
			t.Parallel()
			got, err := builtin.Isolation().Parse(json.RawMessage(`{"label":"zone","max":2}`))
			assert.NoError(t, err, "Parse must accept a configuration of isolation")
			assert.Equal(t, got, config.Parsed(&constraint.IsolationConfig{Label: "zone", Max: 2}),
				"Parse must keep each field that the configuration sets")
		})

		tests := []struct {
			name string
			give string
		}{
			{name: "returns ErrConfig for an unknown field", give: `{"label":"zone","maximum":2}`},
			{name: "returns ErrConfig for data after the JSON value", give: `{"label":"zone"} {}`},
			{name: "returns ErrConfig for a JSON value that is not an object", give: `["zone"]`},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				_, err := builtin.Isolation().Parse(json.RawMessage(tt.give))
				assert.ErrorIs(t, err, builtin.ErrConfig, "Parse must refuse JSON that is not a configuration")
				assert.Equal(t, errs.Classify(err), errs.Invalid, "the refusal of Parse must classify as Invalid")
			})
		}

		t.Run("returns an empty configuration for a strategy without one", func(t *testing.T) {
			t.Parallel()
			got, err := builtin.Singleton().Parse(nil)
			assert.NoError(t, err, "Parse must parse the empty configuration")
			assert.NoError(t, got.Validate(), "the empty configuration must be valid")
			encoded, err := json.Marshal(got)
			assert.NoError(t, err, "the empty configuration must encode")
			assert.Equal(t, string(encoded), emptyJSON, "the empty configuration must encode as an empty object")
		})
	})

	t.Run("Capabilities", func(t *testing.T) {
		t.Parallel()

		t.Run("returns no capabilities for a configuration of another strategy", func(t *testing.T) {
			t.Parallel()
			other, err := builtin.Singleton().Parse(nil)
			assert.NoError(t, err, "Parse must parse the configuration of the singleton")
			assert.Equal(t, builtin.Exclusive().Capabilities(other), config.Capabilities{},
				"Capabilities must ignore a configuration that its Parse did not return")
		})

		t.Run("returns no capabilities for a strategy without any", func(t *testing.T) {
			t.Parallel()
			cfg, err := builtin.Capacity().Parse(nil)
			assert.NoError(t, err, "Parse must parse the defaults of capacity")
			assert.Equal(t, builtin.Capacity().Capabilities(cfg), config.Capabilities{},
				"Capabilities must return nothing for capacity")
		})
	})

	t.Run("New", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrConfig for a configuration of another strategy", func(t *testing.T) {
			t.Parallel()
			other, err := builtin.Singleton().Parse(nil)
			assert.NoError(t, err, "Parse must parse the configuration of the singleton")
			_, err = builtin.Capacity().New(other)
			assert.ErrorIs(t, err, builtin.ErrConfig, "New must refuse a configuration of another strategy")
		})
	})
}

// roundTrip checks that Parse of f returns cfg from the encoding of cfg.
func roundTrip[T any](tb testing.TB, f config.Factory[T], cfg config.Parsed) {
	tb.Helper()
	assert.RoundTrip(tb, func(c config.Parsed) (json.RawMessage, error) {
		return json.Marshal(c)
	}, f.Parse, cfg, "Parse must return the configuration of its own encoding")
}

// parse returns the configuration that f parses from raw, and stops the test when Parse
// fails.
func parse[T any](tb testing.TB, f config.Factory[T], raw string) config.Parsed {
	tb.Helper()
	cfg, err := f.Parse(json.RawMessage(raw))
	assert.NoError(tb, err, "Parse must accept the configuration of the test")
	return cfg
}
