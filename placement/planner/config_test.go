// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package planner_test

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/placement/constraint"
	"go.thesmos.sh/sharder/placement/goal"
	"go.thesmos.sh/sharder/placement/planner"
	"go.thesmos.sh/sharder/shard"
)

// recordedConfigJSON pins the JSON of a configuration with the default search, churn
// and triggers, one constraint and one goal.
const recordedConfigJSON = `{"constraints":[{"name":"drain"}],` +
	`"goals":[{"name":"stickiness","tier":2,"weight":0.25}],` +
	`"search":{"maxMoves":500,"maxEvaluations":200000,"maxSourceShards":5000,"sample":16,"timeout":"1m0s"},` +
	`"churn":{"maxShardFraction":0.09,"perNode":2,"perShard":1},` +
	`"triggers":{"interval":"30s","minImbalance":1.1,"activeAbove":0.25,"onMembership":true}}`

// defaults returns the configuration of the planner with the default search, churn and
// triggers, without constraints and without goals.
func defaults() planner.Config {
	return planner.Config{
		Search: planner.Search{
			MaxMoves:        500,
			MaxEvaluations:  200_000,
			MaxSourceShards: 5_000,
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
	}
}

// TestConfig checks the JSON and the validation of a Config.
func TestConfig(t *testing.T) {
	t.Parallel()

	t.Run("Config", func(t *testing.T) {
		t.Parallel()

		t.Run("encodes as its recorded JSON", func(t *testing.T) {
			t.Parallel()
			cfg := defaults()
			cfg.Constraints = []planner.Selection{{Name: constraint.DrainName}}
			cfg.Goals = []planner.GoalSelection{{Name: goal.StickinessName, Tier: 2, Weight: 0.25}}
			got, err := json.Marshal(&cfg)
			assert.NoError(t, err, "json.Marshal must encode the configuration")
			assert.Equal(t, string(got), recordedConfigJSON, "the JSON of a configuration must keep its recorded form")
		})
	})

	t.Run("Validate", func(t *testing.T) {
		t.Parallel()

		t.Run("returns nil for the defaults", func(t *testing.T) {
			t.Parallel()
			cfg := defaults()
			assert.NoError(t, cfg.Validate(), "Validate must accept the defaults")
		})

		t.Run("returns nil for the bounds of every range", func(t *testing.T) {
			t.Parallel()
			cfg := defaults()
			cfg.Goals = []planner.GoalSelection{{Name: goal.StickinessName, Weight: math.SmallestNonzeroFloat64}}
			cfg.Search = planner.Search{MaxMoves: 1, MaxEvaluations: 1, MaxSourceShards: 1, Sample: 1, Timeout: 1}
			cfg.Churn = planner.Churn{MaxShardFraction: 1}
			cfg.Triggers = planner.Triggers{Interval: 1, MinImbalance: 1, ActiveAbove: 1}
			assert.NoError(t, cfg.Validate(), "Validate must accept every bound")
		})

		invalid := []struct {
			name   string
			change func(cfg *planner.Config)
		}{
			{
				name:   "returns ErrConfig for a constraint without a name",
				change: func(cfg *planner.Config) { cfg.Constraints = []planner.Selection{{}} },
			},
			{
				name: "returns ErrConfig for a goal without a name",
				change: func(cfg *planner.Config) {
					cfg.Goals = []planner.GoalSelection{{Weight: 1}}
				},
			},
			{
				name: "returns ErrConfig for a negative tier",
				change: func(cfg *planner.Config) {
					cfg.Goals = []planner.GoalSelection{{Name: goal.CountName, Tier: -1, Weight: 1}}
				},
			},
			{
				name: "returns ErrConfig for a weight of 0",
				change: func(cfg *planner.Config) {
					cfg.Goals = []planner.GoalSelection{{Name: goal.CountName}}
				},
			},
			{
				name: "returns ErrConfig for a weight that is NaN",
				change: func(cfg *planner.Config) {
					cfg.Goals = []planner.GoalSelection{{Name: goal.CountName, Weight: math.NaN()}}
				},
			},
			{
				name: "returns ErrConfig for an infinite weight",
				change: func(cfg *planner.Config) {
					cfg.Goals = []planner.GoalSelection{{Name: goal.CountName, Weight: math.Inf(1)}}
				},
			},
			{
				name:   "returns ErrConfig for a maxMoves of 0",
				change: func(cfg *planner.Config) { cfg.Search.MaxMoves = 0 },
			},
			{
				name:   "returns ErrConfig for a maxEvaluations of 0",
				change: func(cfg *planner.Config) { cfg.Search.MaxEvaluations = 0 },
			},
			{
				name:   "returns ErrConfig for a maxSourceShards of 0",
				change: func(cfg *planner.Config) { cfg.Search.MaxSourceShards = 0 },
			},
			{name: "returns ErrConfig for a sample of 0", change: func(cfg *planner.Config) { cfg.Search.Sample = 0 }},
			{
				name:   "returns ErrConfig for a timeout of 0",
				change: func(cfg *planner.Config) { cfg.Search.Timeout = 0 },
			},
			{
				name:   "returns ErrConfig for a negative maxShardFraction",
				change: func(cfg *planner.Config) { cfg.Churn.MaxShardFraction = -0.1 },
			},
			{
				name:   "returns ErrConfig for a maxShardFraction above 1",
				change: func(cfg *planner.Config) { cfg.Churn.MaxShardFraction = 1.5 },
			},
			{
				name:   "returns ErrConfig for a maxShardFraction that is NaN",
				change: func(cfg *planner.Config) { cfg.Churn.MaxShardFraction = math.NaN() },
			},
			{
				name:   "returns ErrConfig for a negative perNode",
				change: func(cfg *planner.Config) { cfg.Churn.PerNode = -1 },
			},
			{
				name:   "returns ErrConfig for a negative perShard",
				change: func(cfg *planner.Config) { cfg.Churn.PerShard = -1 },
			},
			{
				name:   "returns ErrConfig for an interval of 0",
				change: func(cfg *planner.Config) { cfg.Triggers.Interval = 0 },
			},
			{
				name:   "returns ErrConfig for a minImbalance below 1",
				change: func(cfg *planner.Config) { cfg.Triggers.MinImbalance = 0.9 },
			},
			{
				name:   "returns ErrConfig for a minImbalance that is NaN",
				change: func(cfg *planner.Config) { cfg.Triggers.MinImbalance = math.NaN() },
			},
			{
				name:   "returns ErrConfig for an infinite minImbalance",
				change: func(cfg *planner.Config) { cfg.Triggers.MinImbalance = math.Inf(1) },
			},
			{
				name:   "returns ErrConfig for a negative activeAbove",
				change: func(cfg *planner.Config) { cfg.Triggers.ActiveAbove = -0.1 },
			},
			{
				name:   "returns ErrConfig for an activeAbove above 1",
				change: func(cfg *planner.Config) { cfg.Triggers.ActiveAbove = 1.5 },
			},
			{
				name:   "returns ErrConfig for an activeAbove that is NaN",
				change: func(cfg *planner.Config) { cfg.Triggers.ActiveAbove = math.NaN() },
			},
		}
		for _, tt := range invalid {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				cfg := defaults()
				tt.change(&cfg)
				err := cfg.Validate()
				expect.ErrorIs(t, err, planner.ErrConfig, "Validate must return ErrConfig")
				expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of Validate must classify as Invalid")
			})
		}

		t.Run("returns ErrConfig for a nil Config", func(t *testing.T) {
			t.Parallel()
			var cfg *planner.Config
			assert.ErrorIs(t, cfg.Validate(), planner.ErrConfig, "Validate must refuse a nil configuration")
		})

		t.Run("returns one error per problem of the zero Config", func(t *testing.T) {
			t.Parallel()
			joined := assert.ErrorAs[interface{ Unwrap() []error }](t, (&planner.Config{}).Validate(),
				"Validate must join the problems of the configuration")
			assert.Length(t, joined.Unwrap(), 7,
				"Validate must report four limits, the timeout, the interval and the minimum imbalance")
		})
	})
}
