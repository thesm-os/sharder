// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placementtest

import (
	"maps"
	"slices"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/history"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/shard"
)

// clients is the number of goroutines that a law of concurrency runs at once.
const clients = 4

// within bounds the wait of a law of concurrency for its goroutines.
const within = time.Minute

// view is what the readers of a State return for its shards and nodes. A law
// compares two views of one state to check that a call did not change it.
type view struct {
	// Replicas maps each shard to its replicas.
	Replicas map[shard.ID][]shard.Replica
	// Shards maps each node to the shards with a replica on it.
	Shards map[shard.NodeID][]shard.ID
	// Load maps each node to its load.
	Load map[shard.NodeID]shard.Load
	// Peak maps each node to its peak load.
	Peak map[shard.NodeID]shard.Load
	// Count maps each node to its peak count.
	Count map[shard.NodeID]int
	// Moved is the number of moves that the state applied.
	Moved int
}

// AssertConstraint checks the laws of placement.Constraint against c, and fails t
// when c breaks one. It runs each law as a parallel subtest of t. The subtest of a
// law has the name of the method that the law constrains.
//
// The laws evaluate c on the states of the inputs that Inputs generates, with the
// moves that Moves lists and the moves whose From equals their To.
func AssertConstraint(t *testing.T, c placement.Constraint) {
	t.Helper()

	t.Run("Name", func(t *testing.T) {
		t.Parallel()

		t.Run("returns a name", func(t *testing.T) {
			t.Parallel()
			assert.NotEmpty(t, c.Name(), "Name must return the registered name of the constraint")
		})
	})

	t.Run("Check", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the same error for the same state and move", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "Check returns the same error for the same state and move", func(p *prop.Case) {
				e := p.Draw(evaluations(), labelMove)
				assert.Equal(p, c.Check(e.st, e.m), c.Check(e.st, e.m),
					"Check must return the same error for the same state and move")
			})
		})

		t.Run("does not change the state", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "Check does not change the state", func(p *prop.Case) {
				e := p.Draw(evaluations(), labelMove)
				assert.Pure(p, func() view { return viewOf(e.st) }, func() { _ = c.Check(e.st, e.m) },
					"Check must not change the state")
			})
		})

		t.Run("returns the same errors from concurrent calls", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "concurrent calls of Check return the errors of sequential calls", func(p *prop.Case) {
				st := newState(p, p.Draw(Inputs(), labelInput))
				assertConcurrent(p, slices.Concat(Moves(st), keeps(st)),
					func(m placement.Move) error { return c.Check(st, m) })
			})
		})
	})
}

// AssertConstraintAllocs checks that Check of c does not allocate, on the states of
// the inputs that Inputs generates. MaxAllocs counts the allocations of the whole
// process, so the caller calls it from a test that does not run in parallel, such as
// Test<Unit>Allocs.
func AssertConstraintAllocs(t *testing.T, c placement.Constraint) {
	t.Helper()
	prop.MaxAllocs(t, func(e evaluation) { _ = c.Check(e.st, e.m) }, 0, "Check must not allocate",
		prop.Using(evaluations()))
}

// viewOf returns a copy of what the readers of st return for its shards and for the
// nodes of their replicas and of the input.
func viewOf(st *placement.State) view {
	v := view{
		Replicas: map[shard.ID][]shard.Replica{},
		Shards:   map[shard.NodeID][]shard.ID{},
		Load:     map[shard.NodeID]shard.Load{},
		Peak:     map[shard.NodeID]shard.Load{},
		Count:    map[shard.NodeID]int{},
		Moved:    st.Moved(),
	}
	nodes := slices.Clone(st.Nodes())
	for _, sh := range st.Shards() {
		v.Replicas[sh] = slices.Clone(st.Replicas(sh))
		for _, r := range slices.Concat(st.Initial(sh), st.Replicas(sh)) {
			nodes = append(nodes, r.Node)
		}
	}
	for _, n := range nodes {
		v.Shards[n] = slices.Clone(st.OnNode(n))
		v.Load[n] = maps.Clone(st.NodeLoad(n))
		v.Peak[n] = maps.Clone(st.PeakLoad(n))
		v.Count[n] = st.PeakCount(n)
	}
	return v
}

// assertConcurrent stops tb unless the calls of call from clients goroutines at once
// return the results of sequential calls, for each move of moves.
func assertConcurrent[R any](tb assert.TB, moves []placement.Move, call func(placement.Move) R) {
	tb.Helper()
	want := make([]R, len(moves))
	for i, m := range moves {
		want[i] = call(m)
	}
	outcomes := history.Concurrently(clients, within, func(int) (any, error) {
		got := make([]R, len(moves))
		for i, m := range moves {
			got[i] = call(m)
		}
		return got, nil
	})
	for _, o := range outcomes {
		assert.True(tb, o.Finished, "every client must finish its calls")
		assert.Equal(tb, o.Output, any(want), "a concurrent call must return the result of a sequential call")
	}
}
