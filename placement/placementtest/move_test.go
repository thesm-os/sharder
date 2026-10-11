// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placementtest_test

import (
	"slices"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/placementtest"
	"go.thesmos.sh/sharder/shard"
)

// labelInput labels the input that a property generates.
const labelInput = "input"

// TestMove checks that Moves lists exactly the moves that Apply accepts.
func TestMove(t *testing.T) {
	t.Parallel()

	t.Run("Moves", func(t *testing.T) {
		t.Parallel()

		t.Run("lists exactly the moves that Apply accepts", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "Moves lists a move exactly when Apply accepts it", func(c *prop.Case) {
				in := c.Draw(placementtest.Inputs(), labelInput)
				st := newState(c, in)
				moves := placementtest.Moves(st)
				shards := append(slices.Clone(st.Shards()), shard.ID{})
				nodes := []shard.NodeID{""}
				for _, n := range in.Nodes {
					nodes = append(nodes, n.ID)
				}
				for n := range in.Draining {
					nodes = append(nodes, n)
				}
				for _, sh := range st.Shards() {
					for _, r := range st.Replicas(sh) {
						nodes = append(nodes, r.Node)
					}
				}
				slices.Sort(nodes)
				nodes = slices.Compact(nodes)
				for _, sh := range shards {
					for _, from := range nodes {
						for _, to := range nodes {
							for role := range shard.Role(shard.RoleCount + 1) {
								m := placement.Move{From: from, To: to, Shard: sh, Role: role}
								if err := st.Apply(m); err != nil {
									assert.NotContains(c, moves, m, "Moves must not list a move that Apply refuses")
									continue
								}
								assert.Contains(c, moves, m, "Moves must list a move that Apply accepts")
								st = newState(c, in)
							}
						}
					}
				}
			})
		})
	})
}

// newState returns the state of in, and stops tb when NewState refuses in.
func newState(tb assert.TB, in *placement.Input) *placement.State {
	tb.Helper()
	st, err := placement.NewState(in)
	assert.NoError(tb, err, "NewState must accept a generated input")
	return st
}
