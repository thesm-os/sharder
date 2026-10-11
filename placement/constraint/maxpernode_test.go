// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package constraint_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/constraint"
	"go.thesmos.sh/sharder/placement/placementtest"
	"go.thesmos.sh/sharder/shard"
)

// TestMaxPerNode checks the validation of a MaxPerNodeConfig, NewMaxPerNode and the
// moves that a MaxPerNode rejects.
func TestMaxPerNode(t *testing.T) {
	t.Parallel()

	t.Run("MaxPerNodeConfig", func(t *testing.T) {
		t.Parallel()

		t.Run("Validate", func(t *testing.T) {
			t.Parallel()

			t.Run("returns nil for a Max of 1 and a role", func(t *testing.T) {
				t.Parallel()
				assert.NoError(t, constraint.MaxPerNodeConfig{Max: 1, Role: shard.RolePrimary}.Validate(),
					"Validate must accept the configuration")
			})

			invalid := []struct {
				name string
				give constraint.MaxPerNodeConfig
			}{
				{name: "returns ErrConfig for a Max of 0", give: constraint.MaxPerNodeConfig{}},
				{
					name: "returns ErrConfig for a role that is not valid",
					give: constraint.MaxPerNodeConfig{Max: 1, Role: shard.RoleLearner + 1},
				},
			}
			for _, tt := range invalid {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					err := tt.give.Validate()
					expect.ErrorIs(t, err, constraint.ErrConfig, "Validate must return ErrConfig")
					expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of Validate must classify as Invalid")
				})
			}

			t.Run("returns one error per problem of a configuration", func(t *testing.T) {
				t.Parallel()
				joined := assert.ErrorAs[interface{ Unwrap() []error }](t,
					constraint.MaxPerNodeConfig{Role: shard.RoleLearner + 1}.Validate(),
					"Validate must join the problems of the configuration")
				assert.Length(t, joined.Unwrap(), 2, "Validate must report the Max and the role")
			})
		})
	})

	t.Run("NewMaxPerNode", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrConfig for a configuration that Validate refuses", func(t *testing.T) {
			t.Parallel()
			_, err := constraint.NewMaxPerNode(constraint.MaxPerNodeConfig{})
			assert.ErrorIs(t, err, constraint.ErrConfig, "NewMaxPerNode must refuse a Max of 0")
		})
	})

	t.Run("Name", func(t *testing.T) {
		t.Parallel()

		t.Run("returns MaxPerNodeName", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, newMaxPerNode(t, 1, shard.RoleNone).Name(), constraint.MaxPerNodeName,
				"Name must return the registered name")
		})
	})

	t.Run("Check", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			max  int
			role shard.Role
			give placement.Move
			want error
		}{
			{
				name: "returns ErrMaxPerNode for a replica above the Max of every role",
				max:  3,
				give: placement.Move{To: n1, Shard: s4, Role: shard.RolePrimary},
				want: constraint.ErrMaxPerNode,
			},
			{
				name: "returns nil for a replica up to the Max of every role",
				max:  3,
				give: placement.Move{To: n3, Shard: s4, Role: shard.RolePrimary},
			},
			{
				name: "returns nil for a replica that keeps a node at its Max",
				max:  3,
				give: placement.Move{From: n1, To: n1, Shard: s1, Role: shard.RolePrimary},
			},
			{
				name: "returns ErrMaxPerNode for a replica that keeps a node above its Max",
				max:  2,
				give: placement.Move{From: n1, To: n1, Shard: s1, Role: shard.RolePrimary},
				want: constraint.ErrMaxPerNode,
			},
			{
				name: "returns ErrMaxPerNode for a replica above the Max of its role",
				max:  1,
				role: shard.RoleSecondary,
				give: placement.Move{To: n2, Shard: s4, Role: shard.RoleSecondary},
				want: constraint.ErrMaxPerNode,
			},
			{
				name: "returns nil for a replica up to the Max of its role",
				max:  1,
				role: shard.RoleSecondary,
				give: placement.Move{From: n2, To: n1, Shard: s1, Role: shard.RoleSecondary},
			},
			{
				name: "returns nil for a replica in another role",
				max:  1,
				role: shard.RoleSecondary,
				give: placement.Move{To: n2, Shard: s4, Role: shard.RolePrimary},
			},
			{
				name: "returns nil for a move that removes a replica",
				max:  1,
				give: placement.Move{From: n1, Shard: s1, Role: shard.RolePrimary},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				got := newMaxPerNode(t, tt.max, tt.role).Check(newState(t, cell()), tt.give)
				assert.Equal(t, got, tt.want, "Check must return the error of the Max")
			})
		}
	})
}

// TestMaxPerNodeConformance checks the laws of placement.Constraint against a
// MaxPerNode.
func TestMaxPerNodeConformance(t *testing.T) {
	t.Parallel()
	placementtest.AssertConstraint(t, newMaxPerNode(t, 2, shard.RoleNone))
}

// TestMaxPerNodeAllocs checks that Check of a MaxPerNode does not allocate.
func TestMaxPerNodeAllocs(t *testing.T) {
	placementtest.AssertConstraintAllocs(t, newMaxPerNode(t, 1, shard.RoleSecondary))
}

// BenchmarkMaxPerNode reports the cost of Check of a MaxPerNode, and fails when it
// allocates.
func BenchmarkMaxPerNode(b *testing.B) {
	b.Run("Check", func(b *testing.B) {
		c := newMaxPerNode(b, 3, shard.RoleNone)
		st := newState(b, cell())
		m := placement.Move{To: n3, Shard: s4, Role: shard.RolePrimary}
		var err error

		contract := bench.Start(b).MaxAllocs(0)
		defer contract.End()

		for contract.Loop() {
			err = c.Check(st, m)
		}

		assert.NoError(b, err, "the benchmark must measure a move up to the Max")
	})
}

// newMaxPerNode returns the constraint max-per-node with the Max most and role, and
// stops tb when NewMaxPerNode refuses them.
func newMaxPerNode(tb assert.TB, most int, role shard.Role) *constraint.MaxPerNode {
	tb.Helper()
	c, err := constraint.NewMaxPerNode(constraint.MaxPerNodeConfig{Max: most, Role: role})
	assert.NoError(tb, err, "NewMaxPerNode must accept the configuration of the test")
	return c
}
