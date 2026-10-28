// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placementtest

import (
	"maps"
	"slices"
	"sync/atomic"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/history"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/conform"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/constraint"
	"go.thesmos.sh/sharder/shard"
)

// clients is the number of goroutines that a check of concurrency runs at once.
const clients = 4

// within bounds the wait of a check of concurrency for its goroutines.
const within = time.Minute

// ConstraintCells are the options of an application of the suite of
// placement.Constraint that draw the states and the moves of the calls of Check from
// the cells that Inputs generates. The suite starts a State from its zero value,
// because its fields are unexported, so RunConstraintBudgets and BenchConstraintSuite
// take these options. The caller must not modify the slice.
var ConstraintCells = []ConstraintOption{
	conform.Using[placement.Constraint](States()),
	conform.Using[placement.Constraint](CellMoves()),
}

// view is what the readers of a State return for its shards and nodes. A check
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

// ConstraintHasAName checks that Name returns the registered name of the constraint.
//
//dokimi:check Constraint.Name defect=dropsTheConstraintName
func ConstraintHasAName(h *ConstraintHandle) {
	assert.NotEmpty(h, h.Subject().Name(), h.Statement())
}

// dropsTheConstraintName returns a planted defect over constraint.Drain whose Name
// returns the empty name.
func dropsTheConstraintName(h *ConstraintHandle) placement.Constraint {
	return NewConstraintStub(h, ConstraintStubDelegateTo(constraint.Drain{}),
		WithConstraintName(func() shard.StrategyName { return "" }))
}

// CheckIsDeterministic checks that Check returns the same error for the same state and
// move.
//
//dokimi:check Constraint.Check defect=driftsChecks
func CheckIsDeterministic(h *ConstraintHandle, c *prop.Case) {
	s := h.Subject()
	e := c.Draw(evaluations(), labelMove)
	assert.Equal(h, s.Check(e.st, e.m), s.Check(e.st, e.m), h.Statement())
}

// driftsChecks returns a planted defect over constraint.Drain whose Check returns the
// other verdict from its second call on: ErrDrain for a destination that does not
// drain, and nil for one that does. A check of concurrency calls Check from more than
// one goroutine, so the count of the calls is atomic.
func driftsChecks(h *ConstraintHandle) placement.Constraint {
	var calls atomic.Uint64
	return NewConstraintStub(h, ConstraintStubDelegateTo(constraint.Drain{}),
		WithConstraintCheck(func(st *placement.State, m placement.Move) error {
			if calls.Add(1) == 1 {
				return constraint.Drain{}.Check(st, m)
			}
			if st.Draining(m.To) {
				return nil
			}
			return constraint.ErrDrain
		}))
}

// CheckIsPure checks that Check does not change the state.
//
//dokimi:check Constraint.Check defect=mutatesStatesInCheck
func CheckIsPure(h *ConstraintHandle, c *prop.Case) {
	s := h.Subject()
	e := c.Draw(evaluations(), labelMove)
	assert.Pure(h, func() view { return viewOf(e.st) }, func() { _ = s.Check(e.st, e.m) }, h.Statement())
}

// mutatesStatesInCheck returns a planted defect over constraint.Drain whose first
// Check touches its state. A check of concurrency calls Check on one goroutine before
// it calls it from more than one, so the change precedes every concurrent call.
func mutatesStatesInCheck(h *ConstraintHandle) placement.Constraint {
	var touched atomic.Bool
	return NewConstraintStub(h, ConstraintStubDelegateTo(constraint.Drain{}),
		WithConstraintCheck(func(st *placement.State, m placement.Move) error {
			if touched.CompareAndSwap(false, true) {
				touch(h, st)
			}
			return constraint.Drain{}.Check(st, m)
		}))
}

// ConcurrentChecksMatchSequentialChecks checks that concurrent calls of Check return
// the errors of sequential calls.
//
//dokimi:check Constraint.Check defect=driftsChecks
func ConcurrentChecksMatchSequentialChecks(h *ConstraintHandle, c *prop.Case) {
	s := h.Subject()
	st := newState(h, c.Draw(Inputs(), labelInput))
	assertConcurrent(h, slices.Concat(Moves(st), keeps(st)),
		func(m placement.Move) error { return s.Check(st, m) }, h.Statement())
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
// return the results of sequential calls, for each move of moves. The comparison of
// the results passes msg as its message.
func assertConcurrent[R any](tb assert.TB, moves []placement.Move, call func(placement.Move) R, msg string) {
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
		assert.Equal(tb, o.Output, any(want), msg)
	}
}
