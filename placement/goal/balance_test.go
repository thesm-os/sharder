// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package goal_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/goal"
	"go.thesmos.sh/sharder/placement/placementtest"
	"go.thesmos.sh/sharder/shard"
)

// TestBalance checks the validation of a BalanceConfig, NewBalance, and the cost and
// the deltas of a Balance.
func TestBalance(t *testing.T) {
	t.Parallel()

	t.Run("BalanceConfig", func(t *testing.T) {
		t.Parallel()

		t.Run("Validate", func(t *testing.T) {
			t.Parallel()

			t.Run("returns nil for a metric", func(t *testing.T) {
				t.Parallel()
				assert.NoError(t, goal.BalanceConfig{Metric: placementtest.CPU}.Validate(),
					"Validate must accept the configuration")
			})

			t.Run("returns ErrConfig for an empty metric", func(t *testing.T) {
				t.Parallel()
				err := goal.BalanceConfig{}.Validate()
				expect.ErrorIs(t, err, goal.ErrConfig, "Validate must return ErrConfig")
				expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of Validate must classify as Invalid")
			})
		})
	})

	t.Run("NewBalance", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrConfig for a configuration that Validate refuses", func(t *testing.T) {
			t.Parallel()
			_, err := goal.NewBalance(goal.BalanceConfig{})
			assert.ErrorIs(t, err, goal.ErrConfig, "NewBalance must refuse an empty metric")
		})
	})

	t.Run("Name", func(t *testing.T) {
		t.Parallel()

		t.Run("returns BalanceName", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, newBalance(t).Name(), goal.BalanceName, "Name must return the registered name")
		})
	})

	t.Run("Cost", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the variance of the utilisation of the nodes with a capacity", func(t *testing.T) {
			t.Parallel()
			assert.CloseTo(t, newBalance(t).Cost(newState(t, cell())), 0.46/3-1.0/9, tolerance,
				"Cost must return the variance of 0.3, 0.6 and 0.1")
		})

		t.Run("returns 0 for a cell without a capacity", func(t *testing.T) {
			t.Parallel()
			in := cell()
			in.Nodes = in.Nodes[3:]
			assert.Equal(t, newBalance(t).Cost(newState(t, in)), 0.0, "Cost must return 0 without a capacity")
		})
	})

	t.Run("Delta", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			give placement.Move
			want float64
		}{
			{
				name: "returns the change of the variance for a move between two nodes",
				give: placement.Move{From: n2, To: n3, Shard: s2, Role: shard.RolePrimary},
				want: -0.04,
			},
			{
				name: "returns the change of the variance for a move to a node without a capacity",
				give: placement.Move{From: n1, To: n4, Shard: s3, Role: shard.RolePrimary},
				want: (0.41/3 - 0.81/9) - (0.46/3 - 1.0/9),
			},
			{
				name: "returns the change of the variance for a move from a draining node",
				give: placement.Move{From: d1, To: n3, Shard: s4, Role: shard.RolePrimary},
				want: (0.5125/3 - 1.3225/9) - (0.46/3 - 1.0/9),
			},
			{
				name: "returns 0 for a move whose From equals its To",
				give: placement.Move{From: n1, To: n1, Shard: s3, Role: shard.RolePrimary},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.CloseTo(t, newBalance(t).Delta(newState(t, cell()), tt.give), tt.want, tolerance,
					"Delta must return the change of the variance")
			})
		}

		t.Run("returns 0 for a cell without a capacity", func(t *testing.T) {
			t.Parallel()
			in := cell()
			in.Nodes = in.Nodes[3:]
			assert.Equal(t, newBalance(t).Delta(newState(t, in), placement.Move{
				From: g1, To: n4, Shard: s5,
				Role: shard.RolePrimary,
			}), 0.0, "Delta must return 0 without a capacity")
		})
	})
}

// balanceSubject returns the balance of the metric cpu, the subject of the suite of
// placement.Goal. It stops h when NewBalance refuses it.
func balanceSubject(h *placementtest.GoalHandle) *goal.Balance {
	return newBalance(h)
}

// newBalance returns the goal balance of the metric cpu, and stops tb when NewBalance
// refuses it.
func newBalance(tb assert.TB) *goal.Balance {
	tb.Helper()
	g, err := goal.NewBalance(goal.BalanceConfig{Metric: placementtest.CPU})
	assert.NoError(tb, err, "NewBalance must accept the metric cpu")
	return g
}
