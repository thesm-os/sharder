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
	"go.thesmos.sh/sharder/shard"
)

// TestSpread checks the validation of a SpreadConfig, NewSpread, and the cost and the
// deltas of a Spread.
func TestSpread(t *testing.T) {
	t.Parallel()

	t.Run("SpreadConfig", func(t *testing.T) {
		t.Parallel()

		t.Run("Validate", func(t *testing.T) {
			t.Parallel()

			t.Run("returns nil for a label", func(t *testing.T) {
				t.Parallel()
				assert.NoError(t, goal.SpreadConfig{Label: placementtest.Zone}.Validate(),
					"Validate must accept the configuration")
			})

			t.Run("returns ErrConfig for an empty label", func(t *testing.T) {
				t.Parallel()
				err := goal.SpreadConfig{}.Validate()
				expect.ErrorIs(t, err, goal.ErrConfig, "Validate must return ErrConfig")
				expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of Validate must classify as Invalid")
			})
		})
	})

	t.Run("NewSpread", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrConfig for a configuration that Validate refuses", func(t *testing.T) {
			t.Parallel()
			_, err := goal.NewSpread(goal.SpreadConfig{})
			assert.ErrorIs(t, err, goal.ErrConfig, "NewSpread must refuse an empty label")
		})
	})

	t.Run("Name", func(t *testing.T) {
		t.Parallel()

		t.Run("returns SpreadName", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, newSpread(t).Name(), goal.SpreadName, "Name must return the registered name")
		})
	})

	t.Run("Cost", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the replicas that share a value with an earlier replica of their shard", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, newSpread(t).Cost(newState(t, cell())), 1.0,
				"Cost must count the second replica of s1 in zone a")
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
				name: "returns -1 for a move of a shared replica to a node without the label",
				give: placement.Move{From: n3, To: n4, Shard: s1, Role: shard.RoleSecondary},
				want: -1,
			},
			{
				name: "returns -1 for a move that removes a shared replica",
				give: placement.Move{From: n3, Shard: s1, Role: shard.RoleSecondary},
				want: -1,
			},
			{
				name: "returns -1 for a move of the first replica of a shared value",
				give: placement.Move{From: n1, To: n4, Shard: s1, Role: shard.RolePrimary},
				want: -1,
			},
			{
				name: "returns 1 for a replica that shares the value of a replica of its shard",
				give: placement.Move{To: n3, Shard: s3, Role: shard.RoleSecondary},
				want: 1,
			},
			{
				name: "returns 0 for a replica under a value of its own",
				give: placement.Move{To: n1, Shard: s2, Role: shard.RoleSecondary},
			},
			{
				name: "returns 0 for a replica on a node without the label",
				give: placement.Move{To: n4, Shard: s3, Role: shard.RoleSecondary},
			},
			{
				name: "returns 0 for a move whose From equals its To",
				give: placement.Move{From: n3, To: n3, Shard: s1, Role: shard.RoleSecondary},
			},
			{
				name: "returns 0 for a move whose From equals its To on a node without a replica of the shard",
				give: placement.Move{From: n3, To: n3, Shard: s3, Role: shard.RoleSecondary},
			},
			{
				name: "returns 0 for a move of an unshared replica to a node without the label",
				give: placement.Move{From: n2, To: n4, Shard: s1, Role: shard.RoleSecondary},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, newSpread(t).Delta(newState(t, cell()), tt.give), tt.want,
					"Delta must return the change of the cost")
			})
		}
	})

	// blank returns the cell of the tests with the empty value of the label zone on n2,
	// and with the replicas of s6 on the nodes, the first in the role primary.
	blank := func(nodes ...shard.NodeID) *placement.Input {
		in := cell()
		in.Nodes[1].Labels[0].Value = ""
		replicas := make([]shard.Replica, len(nodes))
		for i, n := range nodes {
			replicas[i] = shard.Replica{Node: n, Role: shard.RoleSecondary}
		}
		if len(replicas) > 0 {
			replicas[0].Role = shard.RolePrimary
			in.Current.Placements[s6] = shard.Placement{Shard: s6, Replicas: replicas}
		}
		return in
	}

	t.Run("Cost", func(t *testing.T) {
		t.Parallel()

		blanks := []struct {
			name  string
			nodes []shard.NodeID
		}{
			{
				name:  "does not count a replica without the label after a replica under the empty value",
				nodes: []shard.NodeID{n2, n4},
			},
			{
				name:  "does not count a replica under the empty value after a replica without the label",
				nodes: []shard.NodeID{n4, n2},
			},
		}
		for _, tt := range blanks {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, newSpread(t).Cost(newState(t, blank(tt.nodes...))), 1.0,
					"Cost must count only the second replica of s1 in zone a")
			})
		}
	})

	t.Run("Delta", func(t *testing.T) {
		t.Parallel()

		blanks := []struct {
			name string
			give placement.Move
			want float64
		}{
			{
				name: "returns 0 for a replica under the empty value beside a replica on a draining node",
				give: placement.Move{To: n2, Shard: s4, Role: shard.RoleSecondary},
			},
			{
				name: "returns -1 for a move of a shared replica to a node without the label beside the empty value",
				give: placement.Move{From: n3, To: n4, Shard: s1, Role: shard.RoleSecondary},
				want: -1,
			},
		}
		for _, tt := range blanks {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, newSpread(t).Delta(newState(t, blank()), tt.give), tt.want,
					"Delta must not read a missing label as the empty value")
			})
		}
	})
}

// TestSpreadConformance checks the laws of placement.Goal against a Spread.
func TestSpreadConformance(t *testing.T) {
	t.Parallel()
	placementtest.AssertGoal(t, newSpread(t))
}

// TestSpreadAllocs checks that Delta of a Spread does not allocate.
func TestSpreadAllocs(t *testing.T) {
	placementtest.AssertGoalAllocs(t, newSpread(t))
}

// BenchmarkSpread reports the cost of Delta of a Spread for a shard of three replicas,
// and fails when it allocates.
func BenchmarkSpread(b *testing.B) {
	b.Run("Delta", func(b *testing.B) {
		g := newSpread(b)
		st := newState(b, cell())
		m := placement.Move{From: n3, To: n4, Shard: s1, Role: shard.RoleSecondary}
		var got float64

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			got = g.Delta(st, m)
		}

		assert.Equal(b, got, -1.0, "the benchmark must measure a move that spreads the replicas")
	})
}

// newSpread returns the goal spread of the label zone, and stops tb when NewSpread
// refuses it.
func newSpread(tb assert.TB) *goal.Spread {
	tb.Helper()
	g, err := goal.NewSpread(goal.SpreadConfig{Label: placementtest.Zone})
	assert.NoError(tb, err, "NewSpread must accept the label zone")
	return g
}
