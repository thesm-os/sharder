// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package goal_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/goal"
	"go.thesmos.sh/sharder/placement/placementtest"
	"go.thesmos.sh/sharder/replication"
	"go.thesmos.sh/sharder/shard"
)

// TestColocate checks the validation of a ColocateConfig, NewColocate, and the cost and
// the deltas of a Colocate.
func TestColocate(t *testing.T) {
	t.Parallel()

	t.Run("ColocateConfig", func(t *testing.T) {
		t.Parallel()

		t.Run("Validate", func(t *testing.T) {
			t.Parallel()

			t.Run("returns nil for a group", func(t *testing.T) {
				t.Parallel()
				assert.NoError(t, goal.ColocateConfig{Group: placementtest.Tenant}.Validate(),
					"Validate must accept the configuration")
			})

			t.Run("returns ErrConfig for an empty group", func(t *testing.T) {
				t.Parallel()
				err := goal.ColocateConfig{}.Validate()
				expect.ErrorIs(t, err, goal.ErrConfig, "Validate must return ErrConfig")
				expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of Validate must classify as Invalid")
			})
		})
	})

	t.Run("NewColocate", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrConfig for a configuration that Validate refuses", func(t *testing.T) {
			t.Parallel()
			_, err := goal.NewColocate(goal.ColocateConfig{})
			assert.ErrorIs(t, err, goal.ErrConfig, "NewColocate must refuse an empty group")
		})
	})

	t.Run("Name", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ColocateName", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, newColocate(t).Name(), goal.ColocateName, "Name must return the registered name")
		})
	})

	t.Run("Cost", func(t *testing.T) {
		t.Parallel()

		t.Run("returns 0 for groups on the nodes of one shard", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, newColocate(t).Cost(newState(t, cell())), 0.0,
				"Cost must return 0 for t0 on three nodes and t1 on two")
		})

		t.Run("returns the nodes of each group beyond the replicas of one shard", func(t *testing.T) {
			t.Parallel()
			in := cell()
			in.Model = replication.PrimaryOnly
			in.Current.Placements[s1] = shard.Placement{Shard: s1, Replicas: []shard.Replica{
				{Node: n1, Role: shard.RolePrimary},
			}}
			assert.Equal(t, newColocate(t).Cost(newState(t, in)), 2.0,
				"Cost must count n2 for t0 and g1 for t1 beyond one node each")
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
				name: "returns 1 for a replica on a node without the group",
				give: placement.Move{From: n2, To: n4, Shard: s2, Role: shard.RolePrimary},
				want: 1,
			},
			{
				name: "returns 0 for a move off the only replica of the group on a node",
				give: placement.Move{From: n3, To: n4, Shard: s1, Role: shard.RoleSecondary},
			},
			{
				name: "returns 0 for a replica on a node of the group",
				give: placement.Move{To: n1, Shard: s2, Role: shard.RoleSecondary},
			},
			{
				name: "returns 0 for a move that removes a replica",
				give: placement.Move{From: n3, Shard: s1, Role: shard.RoleSecondary},
			},
			{
				name: "returns 0 for a shard without the group",
				give: placement.Move{From: d1, To: n4, Shard: s4, Role: shard.RolePrimary},
			},
			{
				name: "returns 0 for a move whose From equals its To",
				give: placement.Move{From: n2, To: n2, Shard: s2, Role: shard.RolePrimary},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, newColocate(t).Delta(newState(t, cell()), tt.give), tt.want,
					"Delta must return the change of the cost")
			})
		}

		t.Run("returns -1 for a move that joins the replicas of a group", func(t *testing.T) {
			t.Parallel()
			in := cell()
			in.Model = replication.PrimaryOnly
			in.Current.Placements[s1] = shard.Placement{Shard: s1, Replicas: []shard.Replica{
				{Node: n1, Role: shard.RolePrimary},
			}}
			assert.Equal(t, newColocate(t).Delta(newState(t, in), placement.Move{
				From: n2, To: n1, Shard: s2,
				Role: shard.RolePrimary,
			}), -1.0, "Delta must count the node that the move leaves empty of t0")
		})
	})
}

// TestColocateConformance checks the laws of placement.Goal against a Colocate.
func TestColocateConformance(t *testing.T) {
	t.Parallel()
	placementtest.AssertGoal(t, newColocate(t))
}

// TestColocateAllocs checks that Delta of a Colocate does not allocate.
func TestColocateAllocs(t *testing.T) {
	placementtest.AssertGoalAllocs(t, newColocate(t))
}

// BenchmarkColocate reports the cost of Delta of a Colocate, and fails when it
// allocates.
func BenchmarkColocate(b *testing.B) {
	b.Run("Delta", func(b *testing.B) {
		g := newColocate(b)
		st := newState(b, cell())
		m := placement.Move{From: n2, To: n4, Shard: s2, Role: shard.RolePrimary}
		var got float64

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			got = g.Delta(st, m)
		}

		assert.Equal(b, got, 1.0, "the benchmark must measure a move that spreads a group")
	})
}

// newColocate returns the goal colocate of the group tenant, and stops tb when
// NewColocate refuses it.
func newColocate(tb assert.TB) *goal.Colocate {
	tb.Helper()
	g, err := goal.NewColocate(goal.ColocateConfig{Group: placementtest.Tenant})
	assert.NoError(tb, err, "NewColocate must accept the group tenant")
	return g
}
