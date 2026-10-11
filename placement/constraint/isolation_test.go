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

// TestIsolation checks the validation of an IsolationConfig, NewIsolation and the
// moves that an Isolation rejects.
func TestIsolation(t *testing.T) {
	t.Parallel()

	t.Run("IsolationConfig", func(t *testing.T) {
		t.Parallel()

		t.Run("Validate", func(t *testing.T) {
			t.Parallel()

			t.Run("returns nil for a label and a Max of 1", func(t *testing.T) {
				t.Parallel()
				assert.NoError(t, constraint.IsolationConfig{Label: placementtest.Zone, Max: 1}.Validate(),
					"Validate must accept the configuration")
			})

			invalid := []struct {
				name string
				give constraint.IsolationConfig
			}{
				{name: "returns ErrConfig for an empty label", give: constraint.IsolationConfig{Max: 1}},
				{name: "returns ErrConfig for a Max of 0", give: constraint.IsolationConfig{Label: placementtest.Zone}},
			}
			for _, tt := range invalid {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					err := tt.give.Validate()
					expect.ErrorIs(t, err, constraint.ErrConfig, "Validate must return ErrConfig")
					expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of Validate must classify as Invalid")
				})
			}

			t.Run("returns one error per problem of the zero IsolationConfig", func(t *testing.T) {
				t.Parallel()
				joined := assert.ErrorAs[interface{ Unwrap() []error }](t, constraint.IsolationConfig{}.Validate(),
					"Validate must join the problems of the configuration")
				assert.Length(t, joined.Unwrap(), 2, "Validate must report the label and the Max")
			})
		})
	})

	t.Run("NewIsolation", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrConfig for a configuration that Validate refuses", func(t *testing.T) {
			t.Parallel()
			_, err := constraint.NewIsolation(constraint.IsolationConfig{})
			assert.ErrorIs(t, err, constraint.ErrConfig, "NewIsolation must refuse the zero IsolationConfig")
		})
	})

	t.Run("Name", func(t *testing.T) {
		t.Parallel()

		t.Run("returns IsolationName", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, newIsolation(t, 1).Name(), constraint.IsolationName, "Name must return the registered name")
		})
	})

	t.Run("Check", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			max  int
			give placement.Move
			want error
		}{
			{
				name: "returns ErrIsolation for a second replica under a value",
				max:  1,
				give: placement.Move{To: n3, Shard: s1, Role: shard.RoleSecondary},
				want: constraint.ErrIsolation,
			},
			{
				name: "returns nil for a second replica under a Max of 2",
				max:  2,
				give: placement.Move{To: n3, Shard: s1, Role: shard.RoleSecondary},
			},
			{
				name: "returns nil for a replica that relocates under the value it leaves",
				max:  1,
				give: placement.Move{From: n1, To: n3, Shard: s1, Role: shard.RolePrimary},
			},
			{
				name: "returns nil for a replica that keeps one replica under its value",
				max:  1,
				give: placement.Move{From: n1, To: n1, Shard: s1, Role: shard.RolePrimary},
			},
			{
				name: "returns ErrIsolation for a replica that keeps two replicas under its value",
				max:  1,
				give: placement.Move{From: n1, To: n1, Shard: s6, Role: shard.RolePrimary},
				want: constraint.ErrIsolation,
			},
			{
				name: "returns nil for a node without the label",
				max:  1,
				give: placement.Move{To: n4, Shard: s6, Role: shard.RoleSecondary},
			},
			{
				name: "returns nil for a replica beside a replica on a node that the state does not describe",
				max:  1,
				give: placement.Move{To: n1, Shard: s3, Role: shard.RoleSecondary},
			},
			{
				name: "returns nil for a move that removes a replica",
				max:  1,
				give: placement.Move{From: n1, Shard: s6, Role: shard.RolePrimary},
			},
			{
				name: "returns nil for a node that the state does not describe",
				max:  1,
				give: placement.Move{From: d1, To: d1, Shard: s3, Role: shard.RolePrimary},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				got := newIsolation(t, tt.max).Check(newState(t, cell()), tt.give)
				assert.Equal(t, got, tt.want, "Check must return the error of the isolation")
			})
		}

		// blank is the cell of the tests with the empty value of the label zone on n2.
		blank := cell()
		blank.Nodes[1].Labels[0].Value = ""

		blanks := []struct {
			name string
			give placement.Move
		}{
			{
				name: "returns nil for a node without the label beside a replica under the empty value",
				give: placement.Move{To: n4, Shard: s1, Role: shard.RoleSecondary},
			},
			{
				name: "returns nil for a replica under the empty value beside a replica on a node without the label",
				give: placement.Move{To: n2, Shard: s3, Role: shard.RoleSecondary},
			},
		}
		for _, tt := range blanks {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.NoError(t, newIsolation(t, 1).Check(newState(t, blank), tt.give),
					"Check must not count a node without the label under the empty value")
			})
		}
	})
}

// TestIsolationConformance checks the laws of placement.Constraint against an
// Isolation.
func TestIsolationConformance(t *testing.T) {
	t.Parallel()
	placementtest.AssertConstraint(t, newIsolation(t, 1))
}

// TestIsolationAllocs checks that Check of an Isolation does not allocate.
func TestIsolationAllocs(t *testing.T) {
	placementtest.AssertConstraintAllocs(t, newIsolation(t, 1))
}

// BenchmarkIsolation reports the cost of Check of an Isolation, and fails when it
// allocates.
func BenchmarkIsolation(b *testing.B) {
	b.Run("Check", func(b *testing.B) {
		c := newIsolation(b, 1)
		st := newState(b, cell())
		m := placement.Move{From: n1, To: n2, Shard: s6, Role: shard.RolePrimary}
		var err error

		contract := bench.Start(b).MaxAllocs(0)
		defer contract.End()

		for contract.Loop() {
			err = c.Check(st, m)
		}

		assert.NoError(b, err, "the benchmark must measure a move that isolates the replicas")
	})
}

// newIsolation returns the constraint isolation of the label zone with the Max most,
// and stops tb when NewIsolation refuses it.
func newIsolation(tb assert.TB, most int) *constraint.Isolation {
	tb.Helper()
	c, err := constraint.NewIsolation(constraint.IsolationConfig{Label: placementtest.Zone, Max: most})
	assert.NoError(tb, err, "NewIsolation must accept the configuration of the test")
	return c
}
