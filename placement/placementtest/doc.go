// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package placementtest contains the conformance suites and the doubles of
// placement.Constraint, placement.Goal and placement.Placer, the suite and the double of
// placement.State, and the cells that their checks run on.
//
// Every constraint, goal and placer passes the same checks, so the planner and the core
// rely on the laws of package placement under each strategy that a namespace selects.
// [RunConstraintSuite], [RunGoalSuite] and [RunPlacerSuite] apply every check of
// [ConstraintSuite], [GoalSuite] and [PlacerSuite] to a new subject of a factory, one
// subject for each check. Each check belongs to the method that it constrains.
//
// # Cells
//
// The checks run on the inputs that [Inputs] generates: small cells under
// replication.PrimaryOnly or [Replicated], with accepting, draining and gone nodes,
// labels, loads, transitions in flight and faults. A test configures a constraint or a
// goal with the names of their metrics and labels, such as [CPU] and [Zone]. [Moves]
// lists the moves that a state accepts. [States] generates the states of the cells, and
// [CellMoves] the moves between their nodes. An application of the suite of Placer passes
// [PlacerCells] to draw the input of each generated call of Place from the cells.
//
// # Budgets
//
// The budgets of Check and Delta state that they do not allocate. The suites start a
// placement.State from its zero value, because its fields are unexported. An
// application of a budget passes [ConstraintCells] or [GoalCells] to draw the states
// and the moves of the calls from the cells of Inputs.
//
// [StateSuite] is the suite of the hot path placement.State, whose readers constraints
// and goals call for each evaluation of the planner. Its budgets state that the readers
// do not allocate. [RepeatedMovesDoNotAllocate] states that Apply does not allocate for
// a relocation and the relocation back, once both moves ran.
//
// # Planted defects
//
// Each check has a planted defect: the double of its interface over constraint.Drain,
// goal.Stickiness or the planner of those two, with the calls that break the law of the
// check replaced. The planted defect of RepeatedMovesDoNotAllocate is the double of
// State over the state of a cell of two nodes. The test of the suite runs each check on
// its defect, and the check must fail at the assertion whose message is its statement.
// Every other check must pass on the defect, except a check that shares the defect. A
// placer of the checks implements [Constrained], so [PlaceReturnsValidPlans] validates
// its plans against its constraints.
//
// # Doubles
//
// [ConstraintStub], [GoalStub], [PlacerStub] and [StateStub] are recording doubles of
// [placement.Constraint], [placement.Goal], [placement.Placer] and the methods of
// [placement.State]. Each method of a double returns the results of a function, fixed
// results or an armed fault.
//
// # Errors
//
// A check whose subject does not implement Constrained ends with a fault that wraps
// [ErrUnconstrained].
//
// # Dependency position
//
// placementtest imports the standard library, go.dokimi.dev/assert with its packages
// history and prop, go.dokimi.dev/conform, go.dokimi.dev/stub, the packages clock/fake,
// errs, id, rand, rand/pcg and tag of go.thesmos.sh/core, and the packages partition,
// placement, placement/constraint, placement/goal, placement/planner, replication and
// shard of sharder.
//
//dokimi:sentinel
package placementtest
