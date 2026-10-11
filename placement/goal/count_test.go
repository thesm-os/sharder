// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package goal_test

import (
	"encoding/json"
	"math"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/goal"
	"go.thesmos.sh/sharder/placement/placementtest"
	"go.thesmos.sh/sharder/shard"
)

// recordedCountJSON pins the JSON of a configuration of count that weighs a primary 2
// and a secondary 1.
const recordedCountJSON = `{"roleWeights":{"primary":2,"secondary":1}}`

// TestCount checks the validation of a CountConfig, NewCount, and the cost and the
// deltas of a Count.
func TestCount(t *testing.T) {
	t.Parallel()

	t.Run("CountConfig", func(t *testing.T) {
		t.Parallel()

		t.Run("decodes the weights of the roles from their spellings", func(t *testing.T) {
			t.Parallel()
			var cfg goal.CountConfig
			assert.NoError(t, json.Unmarshal([]byte(recordedCountJSON), &cfg), "encoding/json must decode the weights")
			assert.Equal(t, cfg.RoleWeights, map[shard.Role]float64{shard.RolePrimary: 2, shard.RoleSecondary: 1},
				"the weights must have the roles of their spellings")
		})

		t.Run("Validate", func(t *testing.T) {
			t.Parallel()

			t.Run("returns nil for weights of 0 and above", func(t *testing.T) {
				t.Parallel()
				assert.NoError(t, goal.CountConfig{RoleWeights: map[shard.Role]float64{
					shard.RolePrimary:   0,
					shard.RoleSecondary: 3.5,
				}}.Validate(), "Validate must accept the weights")
			})

			invalid := []struct {
				name string
				give map[shard.Role]float64
			}{
				{name: "returns ErrConfig for a weight of RoleNone", give: map[shard.Role]float64{shard.RoleNone: 1}},
				{
					name: "returns ErrConfig for a weight of a role that is not valid",
					give: map[shard.Role]float64{shard.RoleLearner + 1: 1},
				},
				{name: "returns ErrConfig for a negative weight", give: map[shard.Role]float64{shard.RolePrimary: -1}},
				{
					name: "returns ErrConfig for a weight that is NaN",
					give: map[shard.Role]float64{shard.RolePrimary: math.NaN()},
				},
				{
					name: "returns ErrConfig for an infinite weight",
					give: map[shard.Role]float64{shard.RolePrimary: math.Inf(1)},
				},
			}
			for _, tt := range invalid {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					err := goal.CountConfig{RoleWeights: tt.give}.Validate()
					expect.ErrorIs(t, err, goal.ErrConfig, "Validate must return ErrConfig")
					expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of Validate must classify as Invalid")
				})
			}

			t.Run("returns one error per problem of the weights", func(t *testing.T) {
				t.Parallel()
				joined := assert.ErrorAs[interface{ Unwrap() []error }](t, goal.CountConfig{
					RoleWeights: map[shard.Role]float64{shard.RoleNone: -1, shard.RolePrimary: -1},
				}.Validate(), "Validate must join the problems of the weights")
				assert.Length(
					t,
					joined.Unwrap(),
					3,
					"Validate must report the role and the weight of RoleNone and the weight of a primary",
				)
			})
		})
	})

	t.Run("NewCount", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrConfig for a configuration that Validate refuses", func(t *testing.T) {
			t.Parallel()
			_, err := goal.NewCount(goal.CountConfig{RoleWeights: map[shard.Role]float64{shard.RoleNone: 1}})
			assert.ErrorIs(t, err, goal.ErrConfig, "NewCount must refuse a weight of RoleNone")
		})
	})

	t.Run("Name", func(t *testing.T) {
		t.Parallel()

		t.Run("returns CountName", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, newCount(t, nil).Name(), goal.CountName, "Name must return the registered name")
		})
	})

	t.Run("Cost", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the variance of the replica counts of the accepting nodes", func(t *testing.T) {
			t.Parallel()
			assert.CloseTo(t, newCount(t, nil).Cost(newState(t, cell())), 0.6875, tolerance,
				"Cost must return the variance of 2, 2, 1 and 0")
		})

		t.Run("returns the variance of the weighted counts", func(t *testing.T) {
			t.Parallel()
			g := newCount(t, map[shard.Role]float64{shard.RolePrimary: 2})
			assert.CloseTo(
				t,
				g.Cost(newState(t, cell())),
				2.5,
				tolerance,
				"Cost must return the variance of 4, 3, 1 and 0",
			)
		})

		t.Run("returns 0 for a cell without accepting nodes", func(t *testing.T) {
			t.Parallel()
			in := cell()
			in.Nodes = nil
			assert.Equal(t, newCount(t, nil).Cost(newState(t, in)), 0.0, "Cost must return 0 without nodes")
		})
	})

	t.Run("Delta", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name    string
			weights map[shard.Role]float64
			give    placement.Move
			want    float64
		}{
			{
				name: "returns the change of the variance for a move between two accepting nodes",
				give: placement.Move{From: n1, To: n4, Shard: s3, Role: shard.RolePrimary},
				want: -0.5,
			},
			{
				name: "returns the change of the variance for a move from a draining node",
				give: placement.Move{From: d1, To: n4, Shard: s4, Role: shard.RolePrimary},
				want: -0.4375,
			},
			{
				name: "returns the change of the variance for a move that removes a replica",
				give: placement.Move{From: n2, Shard: s2, Role: shard.RolePrimary},
				want: -0.1875,
			},
			{
				name:    "returns the change of the weighted variance",
				weights: map[shard.Role]float64{shard.RolePrimary: 2},
				give:    placement.Move{From: n1, To: n4, Shard: s3, Role: shard.RolePrimary},
				want:    -2,
			},
			{
				name: "returns 0 for a move whose From equals its To",
				give: placement.Move{From: n1, To: n1, Shard: s3, Role: shard.RolePrimary},
			},
			{
				name: "returns 0 for a role that is not valid",
				give: placement.Move{From: n1, To: n4, Shard: s3, Role: shard.RoleLearner + 1},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.CloseTo(t, newCount(t, tt.weights).Delta(newState(t, cell()), tt.give), tt.want, tolerance,
					"Delta must return the change of the variance")
			})
		}

		t.Run("returns 0 for a cell without accepting nodes", func(t *testing.T) {
			t.Parallel()
			in := cell()
			in.Nodes = nil
			assert.Equal(t, newCount(t, nil).Delta(newState(t, in), placement.Move{
				From: n1, Shard: s3,
				Role: shard.RolePrimary,
			}), 0.0, "Delta must return 0 without nodes")
		})
	})
}

// TestCountConformance checks the laws of placement.Goal against a Count.
func TestCountConformance(t *testing.T) {
	t.Parallel()
	placementtest.AssertGoal(t, newCount(t, map[shard.Role]float64{shard.RolePrimary: 2, shard.RoleSecondary: 0.5}))
}

// TestCountAllocs checks that Delta of a Count does not allocate.
func TestCountAllocs(t *testing.T) {
	placementtest.AssertGoalAllocs(t, newCount(t, nil))
}

// BenchmarkCount reports the cost of Delta of a Count, and fails when it allocates.
func BenchmarkCount(b *testing.B) {
	b.Run("Delta", func(b *testing.B) {
		g := newCount(b, nil)
		st := newState(b, cell())
		m := placement.Move{From: n1, To: n4, Shard: s3, Role: shard.RolePrimary}
		var got float64

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			got = g.Delta(st, m)
		}

		assert.CloseTo(b, got, -0.5, tolerance, "the benchmark must measure a move that lowers the variance")
	})
}

// newCount returns the goal count with weights, and stops tb when NewCount refuses
// them.
func newCount(tb assert.TB, weights map[shard.Role]float64) *goal.Count {
	tb.Helper()
	g, err := goal.NewCount(goal.CountConfig{RoleWeights: weights})
	assert.NoError(tb, err, "NewCount must accept the weights of the test")
	return g
}
