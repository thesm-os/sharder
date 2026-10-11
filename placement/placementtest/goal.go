// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placementtest

import (
	"math"
	"slices"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/placement"
)

// maxSteps is the most moves of a sequence that a law of a goal applies.
const maxSteps = 8

// precision is the relative error that a law allows between a delta and the change of
// the cost. A goal computes both with floating-point operations in another order.
const precision = 1e-9

// AssertGoal checks the laws of placement.Goal against g, and fails t when g breaks
// one. It runs each law as a parallel subtest of t. The subtest of a law has the name
// of the method that the law constrains.
//
// The laws evaluate g on the states of the inputs that Inputs generates, with the
// moves that Moves lists and the moves whose From equals their To. A law of Delta
// applies a sequence of moves, so it covers moves away from the start of the pass,
// back to it, and between two other nodes.
func AssertGoal(t *testing.T, g placement.Goal) {
	t.Helper()

	t.Run("Name", func(t *testing.T) {
		t.Parallel()

		t.Run("returns a name", func(t *testing.T) {
			t.Parallel()
			assert.NotEmpty(t, g.Name(), "Name must return the registered name of the goal")
		})
	})

	t.Run("Cost", func(t *testing.T) {
		t.Parallel()

		t.Run("returns a finite cost", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "Cost returns a finite number", func(p *prop.Case) {
				cost := g.Cost(newState(p, p.Draw(Inputs(), labelInput)))
				assert.False(p, math.IsNaN(cost), "Cost must not return NaN")
				assert.False(p, math.IsInf(cost, 0), "Cost must not return an infinity")
			})
		})

		t.Run("returns the same cost for the same state", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "Cost returns the same cost for the same state", func(p *prop.Case) {
				st := newState(p, p.Draw(Inputs(), labelInput))
				assert.Equal(p, g.Cost(st), g.Cost(st), "Cost must return the same cost for the same state")
			})
		})

		t.Run("does not change the state", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "Cost does not change the state", func(p *prop.Case) {
				st := newState(p, p.Draw(Inputs(), labelInput))
				assert.Pure(p, func() view { return viewOf(st) }, func() { _ = g.Cost(st) },
					"Cost must not change the state")
			})
		})
	})

	t.Run("Delta", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the change of Cost for each move of a sequence", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "Delta returns the cost after a move minus the cost before it", func(p *prop.Case) {
				st := newState(p, p.Draw(Inputs(), labelInput))
				for range p.Draw(prop.Integer(1, maxSteps), labelSteps) {
					moves := Moves(st)
					if len(moves) == 0 {
						return
					}
					m := moves[p.Draw(prop.Integer(0, len(moves)-1), labelMove)]
					before := g.Cost(st)
					delta := g.Delta(st, m)
					assert.NoError(p, st.Apply(m), "Apply must accept a move that Moves lists")
					after := g.Cost(st)
					assert.CloseTo(p, delta, after-before, precision*(1+math.Abs(before)+math.Abs(after)),
						"Delta must return the change of Cost that the move makes")
				}
			})
		})

		t.Run("returns 0 for a move whose From equals its To", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "Delta returns 0 for a move that keeps a replica", func(p *prop.Case) {
				st := newState(p, p.Draw(Inputs(), labelInput))
				moves := keeps(st)
				p.Assume(len(moves) > 0)
				m := moves[p.Draw(prop.Integer(0, len(moves)-1), labelMove)]
				assert.Equal(p, g.Delta(st, m), 0.0, "Delta must return 0 for a move whose From equals its To")
			})
		})

		t.Run("does not change the state", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "Delta does not change the state", func(p *prop.Case) {
				e := p.Draw(evaluations(), labelMove)
				assert.Pure(p, func() view { return viewOf(e.st) }, func() { _ = g.Delta(e.st, e.m) },
					"Delta must not change the state")
			})
		})

		t.Run("returns the same deltas from concurrent calls", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "concurrent calls of Delta return the deltas of sequential calls", func(p *prop.Case) {
				st := newState(p, p.Draw(Inputs(), labelInput))
				assertConcurrent(p, slices.Concat(Moves(st), keeps(st)),
					func(m placement.Move) float64 { return g.Delta(st, m) })
			})
		})
	})
}

// AssertGoalAllocs checks that Delta of g does not allocate, on the states of the
// inputs that Inputs generates. MaxAllocs counts the allocations of the whole process,
// so the caller calls it from a test that does not run in parallel, such as
// Test<Unit>Allocs.
func AssertGoalAllocs(t *testing.T, g placement.Goal) {
	t.Helper()
	prop.MaxAllocs(t, func(e evaluation) { _ = g.Delta(e.st, e.m) }, 0, "Delta must not allocate",
		prop.Using(evaluations()))
}
