// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package constraint

import (
	"errors"
	"fmt"
	"slices"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/shard"
)

// LabelsName is the registered name of the constraint labels.
const LabelsName shard.StrategyName = "labels"

// Operator is the comparison of a Rule between the value of a label of a node and the
// values of the rule.
//
// [Operator.IsValid] refuses the zero Operator.
//
// # Encoding
//
// [Operator.String] returns the spelling of an operator, such as "notIn", and the form
// "Operator(n)" for any other value. [ParseOperator] returns the operator of a spelling.
// The text methods write and read the spelling, so encoding/json writes an Operator as a
// string.
//
// # Concurrency
//
// An Operator is a comparable value.
//
//dokimi:enum text=comment
type Operator uint8

const (
	// In admits a node whose label has one of the values of the rule.
	In Operator = 1 // in
	// NotIn admits a node whose label has none of the values of the rule, and a node
	// without the label.
	NotIn Operator = 2 // notIn
	// Exists admits a node with the label.
	Exists Operator = 3 // exists
	// NotExists admits a node without the label.
	NotExists Operator = 4 // notExists
)

// Rule is one rule of the constraint labels. A replica in Role may be on a node only
// when the label Key of the node satisfies Operator with Values.
//
// The zero Rule is not valid, because it has no key and no operator.
//
// # Concurrency
//
// A copy of a Rule shares the elements of Values with the original.
type Rule struct {
	// Key is the key of the label of nodes, such as "region".
	Key string `json:"key"`
	// Values lists the values that In and NotIn compare with. Exists and NotExists
	// take no values.
	Values []string `json:"values,omitempty"`
	// Operator is the comparison of the rule.
	Operator Operator `json:"operator"`
	// Role is the role of the replicas that the rule applies to, or shard.RoleNone for
	// every role.
	Role shard.Role `json:"role,omitempty"`
}

// LabelsConfig is the configuration of the constraint labels.
//
// The zero LabelsConfig has no rules, so the constraint admits every move.
//
// # Concurrency
//
// A copy of a LabelsConfig shares the elements of Rules with the original.
type LabelsConfig struct {
	// Rules lists the rules that every replica must pass.
	Rules []Rule `json:"rules"`
}

// Validate returns an error that joins every problem of c, or nil when c is valid.
// Each problem wraps ErrConfig.
//
// Error modes:
//   - an error that wraps ErrConfig for a rule without a key.
//   - an error that wraps ErrConfig for a rule whose Operator IsValid refuses.
//   - an error that wraps ErrConfig for a rule of In or NotIn without values, and for
//     a rule of Exists or NotExists with values.
//   - an error that wraps ErrConfig for a rule whose Role IsValid refuses.
func (c LabelsConfig) Validate() error {
	var problems []error
	for i, r := range c.Rules {
		if r.Key == "" {
			problems = append(problems, fmt.Errorf("%w: rule %d has no key", ErrConfig, i))
		}
		if !r.Operator.IsValid() {
			problems = append(problems, fmt.Errorf("%w: rule %d has the operator %v", ErrConfig, i, r.Operator))
		}
		if (r.Operator == In || r.Operator == NotIn) && len(r.Values) == 0 {
			problems = append(problems, fmt.Errorf("%w: rule %d of %v has no values", ErrConfig, i, r.Operator))
		}
		if (r.Operator == Exists || r.Operator == NotExists) && len(r.Values) > 0 {
			problems = append(problems, fmt.Errorf("%w: rule %d of %v has values", ErrConfig, i, r.Operator))
		}
		if !r.Role.IsValid() {
			problems = append(problems, fmt.Errorf("%w: rule %d has the role %v", ErrConfig, i, r.Role))
		}
	}
	return errors.Join(problems...)
}

// Labels is the constraint labels. It rejects a replica on a node whose labels fail a
// rule for the role of the replica. [NewLabels] returns a Labels, and the zero Labels
// has no rules.
//
// A move that removes a replica passes, and so does a move to a node that the state
// does not describe.
//
// # Concurrency
//
// A Labels does not change after NewLabels returns it, and it is safe for concurrent
// use.
//
// # Allocation contract
//
// Check does not allocate.
type Labels struct {
	// rules lists the rules, which NewLabels copied.
	rules []Rule
}

var _ placement.Constraint = (*Labels)(nil)

// NewLabels returns the constraint labels with the configuration cfg. It copies the
// rules of cfg.
//
// Error modes:
//   - an error that wraps ErrConfig for each problem that cfg.Validate reports.
func NewLabels(cfg LabelsConfig) (*Labels, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	rules := make([]Rule, len(cfg.Rules))
	for i, r := range cfg.Rules {
		r.Values = slices.Clone(r.Values)
		rules[i] = r
	}
	return &Labels{rules: rules}, nil
}

// Name returns LabelsName.
func (*Labels) Name() shard.StrategyName {
	return LabelsName
}

// Check returns ErrLabels when the destination of m fails a rule for the role of m,
// and nil otherwise.
//
// # Allocation contract
//
// Zero alloc.
func (l *Labels) Check(st *placement.State, m placement.Move) error {
	// The state does not describe the empty node of a removal.
	node, ok := st.Node(m.To)
	if !ok {
		return nil
	}
	for i := range l.rules {
		r := &l.rules[i]
		if r.Role != shard.RoleNone && r.Role != m.Role {
			continue
		}
		label, has := node.Labels.Find(r.Key)
		listed := has && slices.Contains(r.Values, label.Value)
		var admits bool
		switch r.Operator {
		case In:
			admits = listed
		case NotIn:
			admits = !listed
		case Exists:
			admits = has
		default:
			// NewLabels refuses every other operator, so the operator is NotExists.
			admits = !has
		}
		if !admits {
			return ErrLabels
		}
	}
	return nil
}
