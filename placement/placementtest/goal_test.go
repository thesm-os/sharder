// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placementtest_test

import (
	"slices"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/goal"
	"go.thesmos.sh/sharder/placement/placementtest"
	"go.thesmos.sh/sharder/replication"
	"go.thesmos.sh/sharder/shard"
)

// The nodes of the cell of the test of the goal stickiness.
const (
	start shard.NodeID = "start"
	away  shard.NodeID = "away"
	other shard.NodeID = "other"
)

// stickiness is the goal stickiness of package goal, written again from the exported
// methods of placement.State.
type stickiness struct{}

// Name returns the name of the built-in goal stickiness.
func (stickiness) Name() shard.StrategyName {
	return goal.StickinessName
}

// Cost returns the number of replicas whose node and role no replica of their shard
// had at the start of the pass.
func (stickiness) Cost(st *placement.State) float64 {
	cost := 0.0
	for _, sh := range st.Shards() {
		for _, r := range st.Replicas(sh) {
			if !slices.Contains(st.Initial(sh), r) {
				cost++
			}
		}
	}
	return cost
}

// Delta returns the change of the cost that m makes, without applying m.
func (stickiness) Delta(st *placement.State, m placement.Move) float64 {
	if m.From == m.To {
		return 0
	}
	initial := st.Initial(m.Shard)
	delta := 0.0
	if m.From != "" && !slices.Contains(initial, shard.Replica{Node: m.From, Role: m.Role}) {
		delta--
	}
	if m.To != "" && !slices.Contains(initial, shard.Replica{Node: m.To, Role: m.Role}) {
		delta++
	}
	return delta
}

// TestGoal checks that the goal stickiness, written again outside package goal, passes
// the checks of the suite of placement.Goal, and checks its deltas for moves away from
// the start of the pass, back to it, and between two other nodes.
func TestGoal(t *testing.T) {
	t.Parallel()

	t.Run("RunGoalSuite", func(t *testing.T) {
		t.Parallel()
		placementtest.RunGoalSuite(t, func() stickiness { return stickiness{} })
	})

	t.Run("Delta", func(t *testing.T) {
		t.Parallel()

		sh := shard.ID{ID128: id.ID128{15: 1}}
		in := &placement.Input{
			Model:  replication.PrimaryOnly,
			Layout: partition.Layout{Shards: []partition.Shard{{ID: sh}}},
			Current: shard.Assignment{Placements: map[shard.ID]shard.Placement{
				sh: {Shard: sh, Replicas: []shard.Replica{{Node: start, Role: shard.RolePrimary}}},
			}},
			Nodes: []shard.Node{{ID: start}, {ID: away}, {ID: other}},
		}
		st := newState(t, in)
		for _, step := range []struct {
			move placement.Move
			want float64
		}{
			{move: placement.Move{From: start, To: away, Shard: sh, Role: shard.RolePrimary}, want: 1},
			{move: placement.Move{From: away, To: other, Shard: sh, Role: shard.RolePrimary}, want: 0},
			{move: placement.Move{From: other, To: start, Shard: sh, Role: shard.RolePrimary}, want: -1},
		} {
			before := stickiness{}.Cost(st)
			expect.Equal(t, stickiness{}.Delta(st, step.move), step.want, "Delta must return the change of the move")
			assert.NoError(t, st.Apply(step.move), "Apply must accept the move of the test")
			expect.Equal(t, stickiness{}.Cost(st)-before, step.want, "the cost must change by the delta")
		}
	})
}

// TestGoalAllocs checks that Delta of the goal stickiness written again does not
// allocate on the cells of placementtest.Inputs.
func TestGoalAllocs(t *testing.T) {
	placementtest.RunGoalBudgets(t, func(*placementtest.GoalHandle) stickiness { return stickiness{} },
		placementtest.GoalCells...)
}
