// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package goal_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/goal"
	"go.thesmos.sh/sharder/placement/placementtest"
	"go.thesmos.sh/sharder/shard"
)

// TestStickiness checks the cost and the deltas of a Stickiness.
func TestStickiness(t *testing.T) {
	t.Parallel()

	t.Run("Name", func(t *testing.T) {
		t.Parallel()

		t.Run("returns StickinessName", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, goal.Stickiness{}.Name(), goal.StickinessName, "Name must return the registered name")
		})
	})

	t.Run("Cost", func(t *testing.T) {
		t.Parallel()

		t.Run("returns 0 at the start of the pass", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, goal.Stickiness{}.Cost(newState(t, cell())), 0.0, "Cost must count no moved replica")
		})

		t.Run("returns the replicas that differ from the start of the pass", func(t *testing.T) {
			t.Parallel()
			st := newState(t, cell())
			for _, m := range []placement.Move{
				{From: n3, To: n4, Shard: s1, Role: shard.RoleSecondary},
				{To: n3, Shard: s6, Role: shard.RolePrimary},
				{From: n2, Shard: s2, Role: shard.RolePrimary},
			} {
				assert.NoError(t, st.Apply(m), "Apply must accept the moves of the test")
			}
			assert.Equal(t, goal.Stickiness{}.Cost(st), 2.0, "Cost must count the replicas on n4 and n3")
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
				name: "returns 1 for a move away from the start of the pass",
				give: placement.Move{From: n3, To: n4, Shard: s1, Role: shard.RoleSecondary},
				want: 1,
			},
			{
				name: "returns 1 for a move that adds a replica",
				give: placement.Move{To: n3, Shard: s6, Role: shard.RolePrimary},
				want: 1,
			},
			{
				name: "returns 0 for a move that removes a replica of the start of the pass",
				give: placement.Move{From: n2, Shard: s2, Role: shard.RolePrimary},
			},
			{
				name: "returns 0 for a move whose From equals its To",
				give: placement.Move{From: n2, To: n2, Shard: s2, Role: shard.RolePrimary},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, goal.Stickiness{}.Delta(newState(t, cell()), tt.give), tt.want,
					"Delta must return the change of the cost")
			})
		}

		t.Run("returns -1 for a move back to the start of the pass", func(t *testing.T) {
			t.Parallel()
			st := newState(t, cell())
			assert.NoError(t, st.Apply(placement.Move{From: n3, To: n4, Shard: s1, Role: shard.RoleSecondary}),
				"Apply must move the secondary of s1 away")
			assert.Equal(t, goal.Stickiness{}.Delta(st, placement.Move{
				From: n4, To: n3, Shard: s1,
				Role: shard.RoleSecondary,
			}), -1.0, "Delta must count the return of the replica")
		})

		t.Run("returns 0 for a move between two nodes away from the start of the pass", func(t *testing.T) {
			t.Parallel()
			st := newState(t, cell())
			assert.NoError(t, st.Apply(placement.Move{From: n1, To: n4, Shard: s3, Role: shard.RolePrimary}),
				"Apply must move the primary of s3 away")
			assert.Equal(t, goal.Stickiness{}.Delta(st, placement.Move{
				From: n4, To: n3, Shard: s3,
				Role: shard.RolePrimary,
			}), 0.0, "Delta must count no change between two new nodes")
		})
	})
}

// TestStickinessConformance checks the laws of placement.Goal against a Stickiness.
func TestStickinessConformance(t *testing.T) {
	t.Parallel()
	placementtest.AssertGoal(t, goal.Stickiness{})
}

// TestStickinessAllocs checks that Delta of a Stickiness does not allocate.
func TestStickinessAllocs(t *testing.T) {
	placementtest.AssertGoalAllocs(t, goal.Stickiness{})
}

// BenchmarkStickiness reports the cost of Delta of a Stickiness, and fails when it
// allocates.
func BenchmarkStickiness(b *testing.B) {
	b.Run("Delta", func(b *testing.B) {
		st := newState(b, cell())
		m := placement.Move{From: n3, To: n4, Shard: s1, Role: shard.RoleSecondary}
		var got float64

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			got = goal.Stickiness{}.Delta(st, m)
		}

		assert.Equal(b, got, 1.0, "the benchmark must measure a move away from the start of the pass")
	})
}
