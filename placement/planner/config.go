// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package planner

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"go.thesmos.sh/sharder/shard"
)

// Config is the configuration of the planner.
//
// The zero Config is not valid, because its limits of the search are zero.
//
// # Encoding
//
// encoding/json encodes a Config as an object with the keys constraints, goals,
// search, churn and triggers.
//
// # Concurrency
//
// A copy of a Config shares the elements of Constraints and Goals with the original.
type Config struct {
	// Constraints lists the selections of the hard rules of placement, in the order in
	// which the planner checks them.
	Constraints []Selection `json:"constraints"`
	// Goals lists the selections of the soft objectives of placement, each with its
	// tier and its weight.
	Goals []GoalSelection `json:"goals"`
	// Search contains the limits of the work of one pass.
	Search Search `json:"search"`
	// Churn contains the limits of the moves of the improvement of one pass.
	Churn Churn `json:"churn"`
	// Triggers contains the conditions under which a pass runs, and under which a
	// pass improves the assignment.
	Triggers Triggers `json:"triggers"`
}

// Selection is the name and the configuration of one constraint.
//
// # Concurrency
//
// A copy of a Selection shares the bytes of Config with the original.
type Selection struct {
	// Name is the registered name of the constraint, such as "capacity".
	Name shard.StrategyName `json:"name"`
	// Config is the JSON encoding of the configuration of the constraint, such as a
	// constraint.CapacityConfig.
	Config json.RawMessage `json:"config,omitempty"`
}

// GoalSelection is the name, the tier, the weight and the configuration of one goal.
//
// # Concurrency
//
// A copy of a GoalSelection shares the bytes of Config with the original.
type GoalSelection struct {
	// Name is the registered name of the goal, such as "balance".
	Name shard.StrategyName `json:"name"`
	// Config is the JSON encoding of the configuration of the goal, such as a
	// goal.BalanceConfig.
	Config json.RawMessage `json:"config,omitempty"`
	// Tier is the priority of the goal. The planner improves the tiers in ascending
	// order, and never worsens a lower tier to improve a higher one. It is at least 0.
	Tier int `json:"tier"`
	// Weight is the factor of the cost of the goal in the sum of its tier. It is
	// above 0.
	Weight float64 `json:"weight"`
}

// Search contains the limits of the work of one pass. A pass stops at the first of
// them, and returns its plan with Incomplete set.
type Search struct {
	// MaxMoves is the most moves that a pass applies. It is at least 1.
	MaxMoves int `json:"maxMoves"`
	// MaxEvaluations is the most candidate moves that a pass evaluates. It is at least
	// 1.
	MaxEvaluations int `json:"maxEvaluations"`
	// MaxSourceShards is the most shards that a pass examines as the source of a move.
	// It is at least 1.
	MaxSourceShards int `json:"maxSourceShards"`
	// Sample is the number of target nodes that the improvement evaluates for each
	// shard that it examines. It is at least 1.
	Sample int `json:"sample"`
	// Timeout is the most time that a pass takes on the clock of the input. It is
	// above 0.
	Timeout shard.Duration `json:"timeout"`
}

// Churn contains the limits of the moves of the improvement of one pass. The moves of
// repair do not count against the limits apart from PerShard.
type Churn struct {
	// MaxShardFraction is the fraction of the shards of the cell that the improvement
	// may move, from 0 to 1. The planner rounds the number of shards up, so a positive
	// fraction lets the improvement move at least one shard.
	MaxShardFraction float64 `json:"maxShardFraction"`
	// PerNode is the most moves of the improvement out of one node. It is at least 0.
	PerNode int `json:"perNode"`
	// PerShard is the most moves of one shard in a pass. The moves of repair count
	// against it. It is at least 0.
	PerShard int `json:"perShard"`
}

// Triggers contains the conditions under which the controller runs a pass, and under
// which a pass improves the assignment after its repair.
type Triggers struct {
	// Interval is the time between two periodic passes. It is above 0.
	Interval shard.Duration `json:"interval"`
	// MinImbalance is the ratio of node loads below which a pass skips the
	// improvement on a metric. The pass compares it with the ratio of the highest load
	// to the mean, and with the ratio of the mean to the lowest load. It is at least 1.
	MinImbalance float64 `json:"minImbalance"`
	// ActiveAbove is the utilisation below which a pass skips the improvement on a
	// metric whose nodes declare a capacity. It is from 0 to 1.
	ActiveAbove float64 `json:"activeAbove"`
	// OnMembership reports whether a change of the membership of the cell starts a
	// pass.
	OnMembership bool `json:"onMembership"`
}

// Validate returns an error that joins every problem of c, or nil when c is valid.
// Each problem wraps ErrConfig.
//
// Error modes:
//   - an error that wraps ErrConfig for a nil c.
//   - an error that wraps ErrConfig for a constraint or a goal without a name.
//   - an error that wraps ErrConfig for a goal whose tier is negative, or whose weight
//     is not above 0 or not finite.
//   - an error that wraps ErrConfig for a limit of the search below 1, and for a
//     timeout that is not above 0.
//   - an error that wraps ErrConfig for a fraction of the churn outside 0 to 1, and for
//     a negative limit of the churn.
//   - an error that wraps ErrConfig for an interval that is not above 0, for a minimum
//     imbalance below 1 or not finite, and for a threshold of activity outside 0 to 1.
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("%w: no configuration", ErrConfig)
	}
	var problems []error
	for i, s := range c.Constraints {
		if s.Name == "" {
			problems = append(problems, fmt.Errorf("%w: constraint %d has no name", ErrConfig, i))
		}
	}
	for i, g := range c.Goals {
		if g.Name == "" {
			problems = append(problems, fmt.Errorf("%w: goal %d has no name", ErrConfig, i))
		}
		if g.Tier < 0 {
			problems = append(problems, fmt.Errorf("%w: goal %d has the tier %d", ErrConfig, i, g.Tier))
		}
		if !(g.Weight > 0) || math.IsInf(g.Weight, 1) {
			problems = append(problems, fmt.Errorf("%w: goal %d has the weight %v", ErrConfig, i, g.Weight))
		}
	}
	for _, limit := range []struct {
		name  string
		value int
	}{
		{name: "maxMoves", value: c.Search.MaxMoves},
		{name: "maxEvaluations", value: c.Search.MaxEvaluations},
		{name: "maxSourceShards", value: c.Search.MaxSourceShards},
		{name: "sample", value: c.Search.Sample},
	} {
		if limit.value < 1 {
			problems = append(problems, fmt.Errorf("%w: %s %d is below 1", ErrConfig, limit.name, limit.value))
		}
	}
	if c.Search.Timeout <= 0 {
		problems = append(problems,
			fmt.Errorf("%w: the timeout %v is not above 0", ErrConfig, time.Duration(c.Search.Timeout)))
	}
	if !(c.Churn.MaxShardFraction >= 0 && c.Churn.MaxShardFraction <= 1) {
		problems = append(problems,
			fmt.Errorf("%w: maxShardFraction %v is not from 0 to 1", ErrConfig, c.Churn.MaxShardFraction))
	}
	if c.Churn.PerNode < 0 {
		problems = append(problems, fmt.Errorf("%w: perNode %d is below 0", ErrConfig, c.Churn.PerNode))
	}
	if c.Churn.PerShard < 0 {
		problems = append(problems, fmt.Errorf("%w: perShard %d is below 0", ErrConfig, c.Churn.PerShard))
	}
	if c.Triggers.Interval <= 0 {
		problems = append(problems,
			fmt.Errorf("%w: the interval %v is not above 0", ErrConfig, time.Duration(c.Triggers.Interval)))
	}
	if !(c.Triggers.MinImbalance >= 1) || math.IsInf(c.Triggers.MinImbalance, 1) {
		problems = append(problems,
			fmt.Errorf("%w: minImbalance %v is below 1 or not finite", ErrConfig, c.Triggers.MinImbalance))
	}
	if !(c.Triggers.ActiveAbove >= 0 && c.Triggers.ActiveAbove <= 1) {
		problems = append(problems,
			fmt.Errorf("%w: activeAbove %v is not from 0 to 1", ErrConfig, c.Triggers.ActiveAbove))
	}
	return errors.Join(problems...)
}
