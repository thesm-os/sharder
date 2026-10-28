// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package planner

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/shard"
)

// Name is the registered name of the planner.
const Name shard.StrategyName = "planner"

// Planner is the planner. A pass repairs the replicas that need repair, and then
// improves the assignment with a local search over the tiers of goals. [New] returns a
// Planner, and the zero Planner is not ready for use.
//
// # Concurrency
//
// A Planner does not change after New returns it. It is safe for concurrent use when
// its constraints and goals are, and each pass has state of its own.
//
// # Allocation contract
//
// A pass allocates its State, its plan, and a fixed amount for each step of its
// search, so its allocations grow with the cell and with its work limits.
//
//dokimi:subject placement.Placer new=newConstrained options=cells
type Planner struct {
	// constraints lists the hard rules in the order in which a pass checks them.
	constraints []placement.Constraint
	// tiers lists the goals of each tier in ascending tier order.
	tiers []tier
	// search contains the limits of the work of a pass.
	search Search
	// churn contains the limits of the improvement of a pass.
	churn Churn
	// triggers contains the conditions of a pass and of its improvement.
	triggers Triggers
}

// tier is the goals of one tier with their weights.
type tier struct {
	// goals lists the goals of the tier in the order of the configuration.
	goals []placement.Goal
	// weights lists the weight of each goal of goals.
	weights []float64
}

var _ placement.Placer = (*Planner)(nil)

// New returns the planner with the configuration cfg, the constraints and the goals.
// constraints has one constraint for each selection of cfg.Constraints, and goals one
// goal for each selection of cfg.Goals, in the same order and with the same names. The
// planner does not keep cfg.
//
// Error modes:
//   - the errors of cfg.Validate.
//   - an error that wraps ErrConfig when the number of constraints or of goals differs
//     from the number of their selections.
//   - an error that wraps ErrConfig for a nil constraint or goal, and for one whose name
//     differs from the name of its selection.
func New(cfg *Config, constraints []placement.Constraint, goals []placement.Goal) (*Planner, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var problems []error
	if len(constraints) != len(cfg.Constraints) {
		problems = append(problems, fmt.Errorf("%w: %d constraints for %d selections",
			ErrConfig, len(constraints), len(cfg.Constraints)))
	} else {
		for i, c := range constraints {
			if c == nil || c.Name() != cfg.Constraints[i].Name {
				problems = append(problems, fmt.Errorf("%w: constraint %d is not the constraint %q",
					ErrConfig, i, cfg.Constraints[i].Name))
			}
		}
	}
	if len(goals) != len(cfg.Goals) {
		problems = append(problems, fmt.Errorf("%w: %d goals for %d selections", ErrConfig, len(goals), len(cfg.Goals)))
	} else {
		for i, g := range goals {
			if g == nil || g.Name() != cfg.Goals[i].Name {
				problems = append(
					problems,
					fmt.Errorf("%w: goal %d is not the goal %q", ErrConfig, i, cfg.Goals[i].Name),
				)
			}
		}
	}
	if err := errors.Join(problems...); err != nil {
		return nil, err
	}

	levels := make([]int, len(cfg.Goals))
	for i, g := range cfg.Goals {
		levels[i] = g.Tier
	}
	slices.Sort(levels)
	levels = slices.Compact(levels)
	tiers := make([]tier, len(levels))
	for i, g := range cfg.Goals {
		t, _ := slices.BinarySearch(levels, g.Tier)
		tiers[t].goals = append(tiers[t].goals, goals[i])
		tiers[t].weights = append(tiers[t].weights, g.Weight)
	}
	return &Planner{
		constraints: slices.Clone(constraints),
		tiers:       tiers,
		search:      cfg.Search,
		churn:       cfg.Churn,
		triggers:    cfg.Triggers,
	}, nil
}

// Place returns the plan of one pass over in. The pass repairs first. In an emergency
// it repairs only the replicas that are missing or on a gone node, and the replicas
// with a fault. Otherwise it also repairs the replicas on a draining node and the
// replicas that a constraint rejects, and then improves the assignment when the
// triggers report a node at risk. An emergency pass whose repair does not apply a move
// also repairs those replicas, and does not improve the assignment.
//
// Error modes:
//   - an error that wraps the errors of placement.NewState.
//   - an error that wraps placement.ErrInput when in has no clock or no source of
//     randomness, or a Parallelism below 1.
//   - an error that wraps placement.ErrCost when a goal returns a cost or a delta that
//     is not finite.
//   - an error that wraps placement.ErrMove when a constraint or a goal changes the
//     state that it reads.
//   - an error that wraps the cause of ctx when ctx ends before the pass does.
func (pl *Planner) Place(ctx context.Context, in *placement.Input) (placement.Plan, error) {
	if ctx.Err() != nil {
		return placement.Plan{}, context.Cause(ctx)
	}
	st, err := placement.NewState(in)
	if err != nil {
		return placement.Plan{}, fmt.Errorf("planner: state of the input: %w", err)
	}
	if in.Clock == nil || in.Rand == nil || in.Parallelism < 1 {
		return placement.Plan{}, fmt.Errorf("%w: the planner needs a clock, a source of randomness and a Parallelism "+
			"of at least 1", placement.ErrInput)
	}
	p := newPass(pl, in, st)
	return p.run(ctx)
}

// Schedule returns the interval of the triggers, and whether a change of the
// membership starts a pass.
func (pl *Planner) Schedule() placement.Schedule {
	return placement.Schedule{
		Interval:     time.Duration(pl.triggers.Interval),
		OnMembership: pl.triggers.OnMembership,
	}
}
