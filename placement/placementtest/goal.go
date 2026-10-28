// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placementtest

import (
	"math"
	"slices"
	"sync/atomic"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/conform"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/goal"
	"go.thesmos.sh/sharder/shard"
)

// maxSteps is the most moves of a sequence that a check of a goal applies.
const maxSteps = 8

// precision is the relative error that a check allows between a delta and the change
// of the cost. A goal computes both with floating-point operations in another order.
const precision = 1e-9

// GoalCells are the options of an application of the suite of placement.Goal that draw
// the states and the moves of the calls of Cost and Delta from the cells that Inputs
// generates. The suite starts a State from its zero value, because its fields are
// unexported, so RunGoalBudgets and BenchGoalSuite take these options. The caller must
// not modify the slice.
var GoalCells = []GoalOption{
	conform.Using[placement.Goal](States()),
	conform.Using[placement.Goal](CellMoves()),
}

// GoalHasAName checks that Name returns the registered name of the goal.
//
//dokimi:check Goal.Name defect=dropsTheGoalName
func GoalHasAName(h *GoalHandle) {
	assert.NotEmpty(h, h.Subject().Name(), h.Statement())
}

// dropsTheGoalName returns a planted defect over goal.Stickiness whose Name returns the
// empty name.
func dropsTheGoalName(h *GoalHandle) placement.Goal {
	return NewGoalStub(h, GoalStubDelegateTo(goal.Stickiness{}),
		WithGoalName(func() shard.StrategyName { return "" }))
}

// CostIsFinite checks that Cost returns a finite number.
//
//dokimi:check Goal.Cost defect=infiniteEmptyCosts
func CostIsFinite(h *GoalHandle, c *prop.Case) {
	cost := h.Subject().Cost(newState(h, c.Draw(Inputs(), labelInput)))
	assert.False(h, math.IsNaN(cost), h.Statement())
	assert.False(h, math.IsInf(cost, 0), h.Statement())
}

// infiniteEmptyCosts returns a planted defect over goal.Stickiness whose Cost is
// infinite for a cell without shards.
func infiniteEmptyCosts(h *GoalHandle) placement.Goal {
	return NewGoalStub(h, GoalStubDelegateTo(goal.Stickiness{}),
		WithGoalCost(func(st *placement.State) float64 {
			if len(st.Shards()) == 0 {
				return math.Inf(1)
			}
			return goal.Stickiness{}.Cost(st)
		}))
}

// CostIsDeterministic checks that Cost returns the same cost for the same state.
//
//dokimi:check Goal.Cost defect=driftsCosts
func CostIsDeterministic(h *GoalHandle, c *prop.Case) {
	s := h.Subject()
	st := newState(h, c.Draw(Inputs(), labelInput))
	assert.Equal(h, s.Cost(st), s.Cost(st), h.Statement())
}

// driftsCosts returns a planted defect over goal.Stickiness whose Cost adds one when
// its previous call read a state with as many applied moves. One goroutine calls Cost
// in each check, so the count of the previous call has no lock.
func driftsCosts(h *GoalHandle) placement.Goal {
	previous := -1
	return NewGoalStub(h, GoalStubDelegateTo(goal.Stickiness{}),
		WithGoalCost(func(st *placement.State) float64 {
			cost := goal.Stickiness{}.Cost(st)
			if st.Moved() == previous {
				cost++
			}
			previous = st.Moved()
			return cost
		}))
}

// CostIsPure checks that Cost does not change the state.
//
//dokimi:check Goal.Cost defect=mutatesStatesInCost
func CostIsPure(h *GoalHandle, c *prop.Case) {
	s := h.Subject()
	st := newState(h, c.Draw(Inputs(), labelInput))
	assert.Pure(h, func() view { return viewOf(st) }, func() { _ = s.Cost(st) }, h.Statement())
}

// mutatesStatesInCost returns a planted defect over goal.Stickiness whose first Cost
// touches its state.
func mutatesStatesInCost(h *GoalHandle) placement.Goal {
	var touched atomic.Bool
	return NewGoalStub(h, GoalStubDelegateTo(goal.Stickiness{}),
		WithGoalCost(func(st *placement.State) float64 {
			if touched.CompareAndSwap(false, true) {
				touch(h, st)
			}
			return goal.Stickiness{}.Cost(st)
		}))
}

// DeltaMatchesCost checks that Delta returns the cost after a move minus the cost
// before it, for each move of a sequence. The sequence covers moves away from the start
// of the pass, back to it, and between two other nodes.
//
//dokimi:check Goal.Delta defect=missesDeltas
func DeltaMatchesCost(h *GoalHandle, c *prop.Case) {
	s := h.Subject()
	st := newState(h, c.Draw(Inputs(), labelInput))
	before := s.Cost(st)
	for range c.Draw(prop.Integer(1, maxSteps), labelSteps) {
		moves := Moves(st)
		if len(moves) == 0 {
			return
		}
		m := moves[c.Draw(prop.Integer(0, len(moves)-1), labelMove)]
		delta := s.Delta(st, m)
		assert.NoError(h, st.Apply(m), "Apply must accept a move that Moves lists")
		after := s.Cost(st)
		assert.CloseTo(h, delta, after-before, precision*(1+math.Abs(before)+math.Abs(after)), h.Statement())
		before = after
	}
}

// missesDeltas returns a planted defect over goal.Stickiness whose Delta adds one for a
// move whose From differs from its To.
func missesDeltas(h *GoalHandle) placement.Goal {
	return NewGoalStub(h, GoalStubDelegateTo(goal.Stickiness{}),
		WithGoalDelta(func(st *placement.State, m placement.Move) float64 {
			delta := goal.Stickiness{}.Delta(st, m)
			if m.From != m.To {
				delta++
			}
			return delta
		}))
}

// DeltaOfAKeepIsZero checks that Delta returns 0 for a move whose From equals its To.
//
//dokimi:check Goal.Delta defect=chargesKeeps
func DeltaOfAKeepIsZero(h *GoalHandle, c *prop.Case) {
	st := newState(h, c.Draw(Inputs(), labelInput))
	moves := keeps(st)
	c.Assume(len(moves) > 0)
	m := moves[c.Draw(prop.Integer(0, len(moves)-1), labelMove)]
	assert.Equal(h, h.Subject().Delta(st, m), 0.0, h.Statement())
}

// chargesKeeps returns a planted defect over goal.Stickiness whose Delta returns 1 for a
// move whose From equals its To.
func chargesKeeps(h *GoalHandle) placement.Goal {
	return NewGoalStub(h, GoalStubDelegateTo(goal.Stickiness{}),
		WithGoalDelta(func(st *placement.State, m placement.Move) float64 {
			if m.From == m.To {
				return 1
			}
			return goal.Stickiness{}.Delta(st, m)
		}))
}

// DeltaIsPure checks that Delta does not change the state.
//
//dokimi:check Goal.Delta defect=mutatesStatesInDelta
func DeltaIsPure(h *GoalHandle, c *prop.Case) {
	s := h.Subject()
	e := c.Draw(evaluations(), labelMove)
	assert.Pure(h, func() view { return viewOf(e.st) }, func() { _ = s.Delta(e.st, e.m) }, h.Statement())
}

// mutatesStatesInDelta returns a planted defect over goal.Stickiness whose first Delta
// touches its state. A check of concurrency calls Delta on one goroutine before it
// calls it from more than one, so the change precedes every concurrent call.
func mutatesStatesInDelta(h *GoalHandle) placement.Goal {
	var touched atomic.Bool
	return NewGoalStub(h, GoalStubDelegateTo(goal.Stickiness{}),
		WithGoalDelta(func(st *placement.State, m placement.Move) float64 {
			if touched.CompareAndSwap(false, true) {
				touch(h, st)
			}
			return goal.Stickiness{}.Delta(st, m)
		}))
}

// ConcurrentDeltasMatchSequentialDeltas checks that concurrent calls of Delta return the
// deltas of sequential calls.
//
//dokimi:check Goal.Delta defect=driftsDeltas
func ConcurrentDeltasMatchSequentialDeltas(h *GoalHandle, c *prop.Case) {
	s := h.Subject()
	st := newState(h, c.Draw(Inputs(), labelInput))
	assertConcurrent(h, slices.Concat(Moves(st), keeps(st)),
		func(m placement.Move) float64 { return s.Delta(st, m) }, h.Statement())
}

// driftsDeltas returns a planted defect over goal.Stickiness whose Delta adds one after
// its first maxSteps calls. Each other check of Delta calls it at most maxSteps times. A
// check of concurrency calls Delta from more than one goroutine, so the count of the
// calls is atomic.
func driftsDeltas(h *GoalHandle) placement.Goal {
	var calls atomic.Uint64
	return NewGoalStub(h, GoalStubDelegateTo(goal.Stickiness{}),
		WithGoalDelta(func(st *placement.State, m placement.Move) float64 {
			delta := goal.Stickiness{}.Delta(st, m)
			if calls.Add(1) > maxSteps {
				delta++
			}
			return delta
		}))
}
