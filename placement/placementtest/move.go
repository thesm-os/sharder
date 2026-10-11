// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placementtest

import (
	"slices"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/shard"
)

// The labels of the values that the laws generate.
const (
	labelInput = "input"
	labelMove  = "move"
	labelSteps = "steps"
)

// evaluation is a state and a move that a law evaluates on it.
type evaluation struct {
	// st is the state of the evaluation.
	st *placement.State
	// m is the move of the evaluation.
	m placement.Move
}

// Moves returns every move that st.Apply accepts. It lists the moves of each shard
// without a transition in flight, in the order of the IDs of the shards. For each
// replica of a shard it lists the move that removes the replica, and then the moves
// that relocate the replica to each accepting node without a replica of the shard.
// Then, for each role in which the shard has fewer replicas than the Max of the role,
// it lists the moves that add a replica to each such node.
func Moves(st *placement.State) []placement.Move {
	var moves []placement.Move
	for _, sh := range st.Shards() {
		if _, ok := st.InFlight(sh); ok {
			continue
		}
		replicas := st.Replicas(sh)
		var free []shard.NodeID
		for _, n := range st.Nodes() {
			if !slices.ContainsFunc(replicas, func(r shard.Replica) bool { return r.Node == n }) {
				free = append(free, n)
			}
		}
		for _, r := range replicas {
			moves = append(moves, placement.Move{From: r.Node, Shard: sh, Role: r.Role})
			for _, n := range free {
				moves = append(moves, placement.Move{From: r.Node, To: n, Shard: sh, Role: r.Role})
			}
		}
		for role := range shard.Role(shard.RoleCount) {
			count := 0
			for _, r := range replicas {
				if r.Role == role {
					count++
				}
			}
			if count >= st.Max(role) {
				continue
			}
			for _, n := range free {
				moves = append(moves, placement.Move{To: n, Shard: sh, Role: role})
			}
		}
	}
	return moves
}

// keeps returns the move whose From equals its To for each replica of each shard of
// st, in the order of the IDs of the shards and then of their replicas.
func keeps(st *placement.State) []placement.Move {
	var moves []placement.Move
	for _, sh := range st.Shards() {
		for _, r := range st.Replicas(sh) {
			moves = append(moves, placement.Move{From: r.Node, To: r.Node, Shard: sh, Role: r.Role})
		}
	}
	return moves
}

// evaluations returns a generator of a state of a generated input and a move on it:
// a move that Apply accepts, or a move whose From equals its To for a replica of the
// state. It rejects a case whose state has no such move.
func evaluations() prop.Generator[evaluation] {
	return prop.Composite(func(c *prop.Case) evaluation {
		st := newState(c, c.Draw(Inputs(), labelInput))
		moves := slices.Concat(Moves(st), keeps(st))
		c.Assume(len(moves) > 0)
		return evaluation{st: st, m: moves[c.Draw(prop.Integer(0, len(moves)-1), labelMove)]}
	})
}

// newState returns the state of in, and stops tb when NewState refuses in.
func newState(tb assert.TB, in *placement.Input) *placement.State {
	tb.Helper()
	st, err := placement.NewState(in)
	assert.NoError(tb, err, "NewState must accept a generated input")
	return st
}
