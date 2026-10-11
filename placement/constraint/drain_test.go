// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package constraint_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/constraint"
	"go.thesmos.sh/sharder/placement/placementtest"
	"go.thesmos.sh/sharder/shard"
)

// TestDrain checks the moves that a Drain rejects.
func TestDrain(t *testing.T) {
	t.Parallel()

	t.Run("Name", func(t *testing.T) {
		t.Parallel()

		t.Run("returns DrainName", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, constraint.Drain{}.Name(), constraint.DrainName, "Name must return the registered name")
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
				name: "returns ErrDrain for a replica on a draining node",
				give: placement.Move{To: d1, Shard: s4, Role: shard.RolePrimary},
				want: constraint.ErrDrain,
			},
			{
				name: "returns ErrDrain for a replica that remains on a draining node",
				give: placement.Move{From: d1, To: d1, Shard: s3, Role: shard.RolePrimary},
				want: constraint.ErrDrain,
			},
			{
				name: "returns nil for an accepting node",
				give: placement.Move{From: d1, To: n1, Shard: s3, Role: shard.RolePrimary},
			},
			{
				name: "returns nil for a move that removes a replica",
				give: placement.Move{From: d1, Shard: s3, Role: shard.RolePrimary},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				got := constraint.Drain{}.Check(newState(t, cell()), tt.give)
				assert.Equal(t, got, tt.want, "Check must reject a replica on a draining node")
			})
		}
	})
}

// TestDrainConformance checks the laws of placement.Constraint against a Drain.
func TestDrainConformance(t *testing.T) {
	t.Parallel()
	placementtest.AssertConstraint(t, constraint.Drain{})
}

// TestDrainAllocs checks that Check of a Drain does not allocate.
func TestDrainAllocs(t *testing.T) {
	placementtest.AssertConstraintAllocs(t, constraint.Drain{})
}

// BenchmarkDrain reports the cost of Check of a Drain, and fails when it allocates.
func BenchmarkDrain(b *testing.B) {
	b.Run("Check", func(b *testing.B) {
		st := newState(b, cell())
		m := placement.Move{From: d1, To: n1, Shard: s3, Role: shard.RolePrimary}
		var err error

		contract := bench.Start(b).MaxAllocs(0)
		defer contract.End()

		for contract.Loop() {
			err = constraint.Drain{}.Check(st, m)
		}

		assert.NoError(b, err, "the benchmark must measure a move to an accepting node")
	})
}
