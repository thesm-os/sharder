// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin

import (
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/goal"
	"go.thesmos.sh/sharder/shard"
)

// defaultUtilisationAbove is the default of UtilisationConfig.Above.
const defaultUtilisationAbove = 0.9

// Balance returns the factory of the goal balance, which parses a goal.BalanceConfig.
// The metric has no default.
func Balance() config.Factory[placement.Goal] {
	return goalFactory(goal.BalanceName, nil, goal.NewBalance)
}

// Colocate returns the factory of the goal colocate, which parses a
// goal.ColocateConfig. The group has no default.
func Colocate() config.Factory[placement.Goal] {
	return goalFactory(goal.ColocateName, nil, goal.NewColocate)
}

// Count returns the factory of the goal count, which parses a goal.CountConfig. Every
// role has the weight 1 by default.
func Count() config.Factory[placement.Goal] {
	return goalFactory(goal.CountName, nil, goal.NewCount)
}

// Prefer returns the factory of the goal prefer, which parses a goal.PreferConfig. The
// labels have no defaults.
func Prefer() config.Factory[placement.Goal] {
	return goalFactory(goal.PreferName, nil, goal.NewPrefer)
}

// Spread returns the factory of the goal spread, which parses a goal.SpreadConfig. The
// label has no default.
func Spread() config.Factory[placement.Goal] {
	return goalFactory(goal.SpreadName, nil, goal.NewSpread)
}

// Stickiness returns the factory of the goal stickiness, which has no configuration.
func Stickiness() config.Factory[placement.Goal] {
	return goalFactory(goal.StickinessName, nil, func(empty) (goal.Stickiness, error) {
		return goal.Stickiness{}, nil
	})
}

// Utilisation returns the factory of the goal utilisation, which parses a
// goal.UtilisationConfig. The default threshold above is 0.9, and the metric has no
// default.
func Utilisation() config.Factory[placement.Goal] {
	return goalFactory(goal.UtilisationName, func(c *goal.UtilisationConfig) {
		*c = goal.UtilisationConfig{Above: defaultUtilisationAbove}
	}, goal.NewUtilisation)
}

// goalFactory returns the factory of the goal name, which build builds from a
// configuration of type C. defaults sets the defaults of a configuration, and a nil
// defaults leaves the zero C. A goal does not provide or require a capability.
//
// New returns an error that wraps ErrConfig and the error of build.
func goalFactory[C any, P interface {
	*C
	config.Parsed
}, S placement.Goal](
	name shard.StrategyName,
	defaults func(c P),
	build func(c C) (S, error),
) config.Factory[placement.Goal] {
	return &factory[placement.Goal, C, P]{
		name:     name,
		defaults: defaults,
		build: func(c P) (placement.Goal, error) {
			return built[placement.Goal](build(*c))
		},
	}
}
