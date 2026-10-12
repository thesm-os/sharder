// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin

import (
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/constraint"
	"go.thesmos.sh/sharder/shard"
)

// defaultIsolationMax is the default of IsolationConfig.Max.
const defaultIsolationMax = 1

// Capacity returns the factory of the constraint capacity, which parses a
// constraint.CapacityConfig. The default headroom is 0.
func Capacity() config.Factory[placement.Constraint] {
	return constraintFactory(constraint.CapacityName, nil, constraint.NewCapacity)
}

// Drain returns the factory of the constraint drain, which has no configuration.
func Drain() config.Factory[placement.Constraint] {
	return constraintFactory(constraint.DrainName, nil, func(empty) (constraint.Drain, error) {
		return constraint.Drain{}, nil
	})
}

// Isolation returns the factory of the constraint isolation, which parses a
// constraint.IsolationConfig. The default max is 1, and the label has no default.
func Isolation() config.Factory[placement.Constraint] {
	return constraintFactory(constraint.IsolationName, func(c *constraint.IsolationConfig) {
		*c = constraint.IsolationConfig{Max: defaultIsolationMax}
	}, constraint.NewIsolation)
}

// Labels returns the factory of the constraint labels, which parses a
// constraint.LabelsConfig. The constraint has no rules by default.
func Labels() config.Factory[placement.Constraint] {
	return constraintFactory(constraint.LabelsName, nil, constraint.NewLabels)
}

// MatchLabel returns the factory of the constraint match-label, which parses a
// constraint.MatchLabelConfig. The label has no default.
func MatchLabel() config.Factory[placement.Constraint] {
	return constraintFactory(constraint.MatchLabelName, nil, constraint.NewMatchLabel)
}

// MaxPerNode returns the factory of the constraint max-per-node, which parses a
// constraint.MaxPerNodeConfig. The max has no default, and the constraint counts the
// replicas of every role by default.
func MaxPerNode() config.Factory[placement.Constraint] {
	return constraintFactory(constraint.MaxPerNodeName, nil, constraint.NewMaxPerNode)
}

// constraintFactory returns the factory of the constraint name, which build builds from
// a configuration of type C. defaults sets the defaults of a configuration, and a nil
// defaults leaves the zero C. A constraint does not provide or require a capability.
//
// New returns an error that wraps ErrConfig and the error of build.
func constraintFactory[C any, P interface {
	*C
	config.Parsed
}, S placement.Constraint](
	name shard.StrategyName,
	defaults func(c P),
	build func(c C) (S, error),
) config.Factory[placement.Constraint] {
	return &factory[placement.Constraint, C, P]{
		name:     name,
		defaults: defaults,
		build: func(c P) (placement.Constraint, error) {
			return built[placement.Constraint](build(*c))
		},
	}
}
