// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package goal_test

import (
	"math"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/goal"
	"go.thesmos.sh/sharder/placement/placementtest"
	"go.thesmos.sh/sharder/shard"
)

// TestUtilisation checks the validation of a UtilisationConfig, NewUtilisation, and the
// cost and the deltas of a Utilisation.
func TestUtilisation(t *testing.T) {
	t.Parallel()

	t.Run("UtilisationConfig", func(t *testing.T) {
		t.Parallel()

		t.Run("Validate", func(t *testing.T) {
			t.Parallel()

			t.Run("returns nil for a metric and a threshold of 0", func(t *testing.T) {
				t.Parallel()
				assert.NoError(t, goal.UtilisationConfig{Metric: placementtest.CPU}.Validate(),
					"Validate must accept the configuration")
			})

			invalid := []struct {
				name string
				give goal.UtilisationConfig
			}{
				{name: "returns ErrConfig for an empty metric", give: goal.UtilisationConfig{Above: 0.9}},
				{
					name: "returns ErrConfig for a negative threshold",
					give: goal.UtilisationConfig{Metric: placementtest.CPU, Above: -0.1},
				},
				{
					name: "returns ErrConfig for a threshold that is NaN",
					give: goal.UtilisationConfig{Metric: placementtest.CPU, Above: math.NaN()},
				},
				{
					name: "returns ErrConfig for an infinite threshold",
					give: goal.UtilisationConfig{Metric: placementtest.CPU, Above: math.Inf(1)},
				},
			}
			for _, tt := range invalid {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					err := tt.give.Validate()
					expect.ErrorIs(t, err, goal.ErrConfig, "Validate must return ErrConfig")
					expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of Validate must classify as Invalid")
				})
			}

			t.Run("returns one error per problem of a configuration", func(t *testing.T) {
				t.Parallel()
				joined := assert.ErrorAs[interface{ Unwrap() []error }](t, goal.UtilisationConfig{Above: -1}.Validate(),
					"Validate must join the problems of the configuration")
				assert.Length(t, joined.Unwrap(), 2, "Validate must report the metric and the threshold")
			})
		})
	})

	t.Run("NewUtilisation", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrConfig for a configuration that Validate refuses", func(t *testing.T) {
			t.Parallel()
			_, err := goal.NewUtilisation(goal.UtilisationConfig{})
			assert.ErrorIs(t, err, goal.ErrConfig, "NewUtilisation must refuse an empty metric")
		})
	})

	t.Run("Name", func(t *testing.T) {
		t.Parallel()

		t.Run("returns UtilisationName", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, newUtilisation(t).Name(), goal.UtilisationName, "Name must return the registered name")
		})
	})

	t.Run("Cost", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the utilisation above the threshold", func(t *testing.T) {
			t.Parallel()
			assert.CloseTo(t, newUtilisation(t).Cost(newState(t, cell())), 0.1, tolerance,
				"Cost must return the 0.1 of n2 above 0.5")
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
				name: "returns the change of the cost for a move off a node above the threshold",
				give: placement.Move{From: n2, To: n3, Shard: s2, Role: shard.RolePrimary},
				want: -0.1,
			},
			{
				name: "returns the change of the cost for a move onto a node above the threshold",
				give: placement.Move{From: d1, To: n2, Shard: s4, Role: shard.RolePrimary},
				want: 0.3,
			},
			{
				name: "returns 0 for a move whose From equals its To",
				give: placement.Move{From: n2, To: n2, Shard: s2, Role: shard.RolePrimary},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.CloseTo(t, newUtilisation(t).Delta(newState(t, cell()), tt.give), tt.want, tolerance,
					"Delta must return the change of the cost")
			})
		}
	})
}

// utilisationSubject returns the utilisation of the metric cpu above 0.5, the subject
// of the suite of placement.Goal. It stops h when NewUtilisation refuses it.
func utilisationSubject(h *placementtest.GoalHandle) *goal.Utilisation {
	return newUtilisation(h)
}

// newUtilisation returns the goal utilisation of the metric cpu above 0.5, and stops tb
// when NewUtilisation refuses it.
func newUtilisation(tb assert.TB) *goal.Utilisation {
	tb.Helper()
	g, err := goal.NewUtilisation(goal.UtilisationConfig{Metric: placementtest.CPU, Above: 0.5})
	assert.NoError(tb, err, "NewUtilisation must accept the metric cpu above 0.5")
	return g
}
