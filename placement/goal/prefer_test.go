// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package goal_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/tag"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/goal"
	"go.thesmos.sh/sharder/placement/placementtest"
	"go.thesmos.sh/sharder/shard"
)

// TestPrefer checks the validation of a PreferConfig, NewPrefer, and the cost and the
// deltas of a Prefer.
func TestPrefer(t *testing.T) {
	t.Parallel()

	t.Run("PreferConfig", func(t *testing.T) {
		t.Parallel()

		t.Run("Validate", func(t *testing.T) {
			t.Parallel()

			t.Run("returns nil for two labels", func(t *testing.T) {
				t.Parallel()
				assert.NoError(t, goal.PreferConfig{Label: region, From: placementtest.Home}.Validate(),
					"Validate must accept the configuration")
			})

			invalid := []struct {
				name string
				give goal.PreferConfig
			}{
				{name: "returns ErrConfig for an empty label", give: goal.PreferConfig{From: placementtest.Home}},
				{name: "returns ErrConfig for an empty from", give: goal.PreferConfig{Label: region}},
			}
			for _, tt := range invalid {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					err := tt.give.Validate()
					expect.ErrorIs(t, err, goal.ErrConfig, "Validate must return ErrConfig")
					expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of Validate must classify as Invalid")
				})
			}

			t.Run("returns one error per problem of the zero PreferConfig", func(t *testing.T) {
				t.Parallel()
				joined := assert.ErrorAs[interface{ Unwrap() []error }](t, goal.PreferConfig{}.Validate(),
					"Validate must join the problems of the configuration")
				assert.Length(t, joined.Unwrap(), 2, "Validate must report the label and from")
			})
		})
	})

	t.Run("NewPrefer", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrConfig for a configuration that Validate refuses", func(t *testing.T) {
			t.Parallel()
			_, err := goal.NewPrefer(goal.PreferConfig{})
			assert.ErrorIs(t, err, goal.ErrConfig, "NewPrefer must refuse the zero PreferConfig")
		})
	})

	t.Run("Name", func(t *testing.T) {
		t.Parallel()

		t.Run("returns PreferName", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, newPrefer(t).Name(), goal.PreferName, "Name must return the registered name")
		})
	})

	t.Run("Cost", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the replicas outside the preferred values", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, newPrefer(t).Cost(newState(t, cell())), 2.0,
				"Cost must count the replica of s1 in us and the replica of s3 in eu")
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
				name: "returns -1 for a move to the preferred value",
				give: placement.Move{From: n1, To: n3, Shard: s3, Role: shard.RolePrimary},
				want: -1,
			},
			{
				name: "returns 1 for a move away from the preferred value",
				give: placement.Move{From: n1, To: n3, Shard: s1, Role: shard.RolePrimary},
				want: 1,
			},
			{
				name: "returns 0 for a move between two nodes outside the preferred value",
				give: placement.Move{From: n3, To: n4, Shard: s1, Role: shard.RoleSecondary},
			},
			{
				name: "returns -1 for a move that removes a replica outside the preferred value",
				give: placement.Move{From: n3, Shard: s1, Role: shard.RoleSecondary},
				want: -1,
			},
			{
				name: "returns 1 for a replica on a node that the state does not describe",
				give: placement.Move{To: d1, Shard: s3, Role: shard.RoleSecondary},
				want: 1,
			},
			{
				name: "returns 0 for a shard without the label from",
				give: placement.Move{From: n2, To: n3, Shard: s2, Role: shard.RolePrimary},
			},
			{
				name: "returns 0 for a move whose From equals its To",
				give: placement.Move{From: n3, To: n3, Shard: s1, Role: shard.RoleSecondary},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, newPrefer(t).Delta(newState(t, cell()), tt.give), tt.want,
					"Delta must return the change of the cost")
			})
		}

		t.Run("returns 0 for a move to a node without the label under the empty preferred value", func(t *testing.T) {
			t.Parallel()
			in := cell()
			in.Layout.Shards[0].Labels = tag.Tags{t0, {Key: placementtest.Home}}
			m := placement.Move{From: n3, To: n4, Shard: s1, Role: shard.RoleSecondary}
			assert.Equal(t, newPrefer(t).Delta(newState(t, in), m), 0.0,
				"Delta must not read a missing label as the empty value")
		})
	})
}

// preferSubject returns the prefer of the label zone from the shard label home, the
// subject of the suite of placement.Goal. It stops h when NewPrefer refuses it.
func preferSubject(h *placementtest.GoalHandle) *goal.Prefer {
	g, err := goal.NewPrefer(goal.PreferConfig{Label: placementtest.Zone, From: placementtest.Home})
	assert.NoError(h, err, "NewPrefer must accept the labels zone and home")
	return g
}

// newPrefer returns the goal prefer of the label region from the shard label home, and
// stops tb when NewPrefer refuses it.
func newPrefer(tb assert.TB) *goal.Prefer {
	tb.Helper()
	g, err := goal.NewPrefer(goal.PreferConfig{Label: region, From: placementtest.Home})
	assert.NoError(tb, err, "NewPrefer must accept the labels region and home")
	return g
}
