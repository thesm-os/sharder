// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package constraint_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/tag"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/constraint"
	"go.thesmos.sh/sharder/placement/placementtest"
	"go.thesmos.sh/sharder/shard"
)

// TestMatchLabel checks the validation of a MatchLabelConfig, NewMatchLabel and the
// moves that a MatchLabel rejects.
func TestMatchLabel(t *testing.T) {
	t.Parallel()

	t.Run("MatchLabelConfig", func(t *testing.T) {
		t.Parallel()

		t.Run("Validate", func(t *testing.T) {
			t.Parallel()

			t.Run("returns nil for a label", func(t *testing.T) {
				t.Parallel()
				assert.NoError(t, constraint.MatchLabelConfig{Label: jurisdiction}.Validate(),
					"Validate must accept the configuration")
			})

			t.Run("returns ErrConfig for an empty label", func(t *testing.T) {
				t.Parallel()
				err := constraint.MatchLabelConfig{}.Validate()
				expect.ErrorIs(t, err, constraint.ErrConfig, "Validate must return ErrConfig")
				expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of Validate must classify as Invalid")
			})
		})
	})

	t.Run("NewMatchLabel", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrConfig for a configuration that Validate refuses", func(t *testing.T) {
			t.Parallel()
			_, err := constraint.NewMatchLabel(constraint.MatchLabelConfig{})
			assert.ErrorIs(t, err, constraint.ErrConfig, "NewMatchLabel must refuse an empty label")
		})
	})

	t.Run("Name", func(t *testing.T) {
		t.Parallel()

		t.Run("returns MatchLabelName", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, newMatchLabel(t, jurisdiction).Name(), constraint.MatchLabelName,
				"Name must return the registered name")
		})
	})

	t.Run("Check", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			give placement.Move
			want error
		}{
			{
				name: "returns nil for a node with the value of the shard",
				give: placement.Move{To: n2, Shard: s1, Role: shard.RoleSecondary},
			},
			{
				name: "returns nil for a replica that remains on a node with the value of the shard",
				give: placement.Move{From: n3, To: n3, Shard: s2, Role: shard.RolePrimary},
			},
			{
				name: "returns ErrMatchLabel for a node with another value",
				give: placement.Move{From: n2, To: n3, Shard: s1, Role: shard.RoleSecondary},
				want: constraint.ErrMatchLabel,
			},
			{
				name: "returns ErrMatchLabel for a node without the label",
				give: placement.Move{From: n2, To: n4, Shard: s1, Role: shard.RoleSecondary},
				want: constraint.ErrMatchLabel,
			},
			{
				name: "returns ErrMatchLabel for a shard without the label",
				give: placement.Move{To: n1, Shard: s4, Role: shard.RolePrimary},
				want: constraint.ErrMatchLabel,
			},
			{
				name: "returns ErrMatchLabel for a shard that the layout does not list",
				give: placement.Move{To: n1, Shard: s9, Role: shard.RolePrimary},
				want: constraint.ErrMatchLabel,
			},
			{
				name: "returns nil for a node that the state does not describe",
				give: placement.Move{From: n1, To: d1, Shard: s1, Role: shard.RolePrimary},
			},
			{
				name: "returns nil for a move that removes a replica",
				give: placement.Move{From: d1, Shard: s3, Role: shard.RolePrimary},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				got := newMatchLabel(t, jurisdiction).Check(newState(t, cell()), tt.give)
				assert.Equal(t, got, tt.want, "Check must return the error of the label")
			})
		}

		blanks := []struct {
			name   string
			change func(in *placement.Input)
			give   placement.Move
		}{
			{
				name:   "returns ErrMatchLabel for a shard without the label on a node with the empty value",
				change: func(in *placement.Input) { in.Nodes[1].Labels[2].Value = "" },
				give:   placement.Move{To: n2, Shard: s4, Role: shard.RolePrimary},
			},
			{
				name:   "returns ErrMatchLabel for a shard with the empty value on a node without the label",
				change: func(in *placement.Input) { in.Layout.Shards[3].Labels = tag.Tags{{Key: jurisdiction}} },
				give:   placement.Move{To: n4, Shard: s4, Role: shard.RolePrimary},
			},
		}
		for _, tt := range blanks {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				in := cell()
				tt.change(in)
				assert.ErrorIs(t, newMatchLabel(t, jurisdiction).Check(newState(t, in), tt.give),
					constraint.ErrMatchLabel, "Check must not match a missing label with the empty value")
			})
		}
	})
}

// TestMatchLabelConformance checks the laws of placement.Constraint against a
// MatchLabel.
func TestMatchLabelConformance(t *testing.T) {
	t.Parallel()
	placementtest.AssertConstraint(t, newMatchLabel(t, placementtest.Home))
}

// TestMatchLabelAllocs checks that Check of a MatchLabel does not allocate.
func TestMatchLabelAllocs(t *testing.T) {
	placementtest.AssertConstraintAllocs(t, newMatchLabel(t, placementtest.Home))
}

// BenchmarkMatchLabel reports the cost of Check of a MatchLabel, and fails when it
// allocates.
func BenchmarkMatchLabel(b *testing.B) {
	b.Run("Check", func(b *testing.B) {
		c := newMatchLabel(b, jurisdiction)
		st := newState(b, cell())
		m := placement.Move{To: n2, Shard: s1, Role: shard.RoleSecondary}
		var err error

		contract := bench.Start(b).MaxAllocs(0)
		defer contract.End()

		for contract.Loop() {
			err = c.Check(st, m)
		}

		assert.NoError(b, err, "the benchmark must measure a node with the value of the shard")
	})
}

// newMatchLabel returns the constraint match-label of label, and stops tb when
// NewMatchLabel refuses it.
func newMatchLabel(tb assert.TB, label string) *constraint.MatchLabel {
	tb.Helper()
	c, err := constraint.NewMatchLabel(constraint.MatchLabelConfig{Label: label})
	assert.NoError(tb, err, "NewMatchLabel must accept the label of the test")
	return c
}
