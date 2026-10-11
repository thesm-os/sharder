// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package constraint

import (
	"errors"
	"fmt"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/shard"
)

// IsolationName is the registered name of the constraint isolation.
const IsolationName shard.StrategyName = "isolation"

// IsolationConfig is the configuration of the constraint isolation.
//
// The zero IsolationConfig is not valid, because it has no label and a Max of 0.
type IsolationConfig struct {
	// Label is the key of a label of nodes, such as "zone". The constraint counts the
	// replicas of a shard under each value of the label.
	Label string `json:"label"`
	// Max is the most replicas of one shard under one value of Label. It is at least
	// one.
	Max int `json:"max"`
}

// Validate returns an error that joins every problem of c, or nil when c is valid.
// Each problem wraps ErrConfig.
//
// Error modes:
//   - an error that wraps ErrConfig when Label is empty.
//   - an error that wraps ErrConfig when Max is below 1.
func (c IsolationConfig) Validate() error {
	var problems []error
	if c.Label == "" {
		problems = append(problems, fmt.Errorf("%w: label is empty", ErrConfig))
	}
	if c.Max < 1 {
		problems = append(problems, fmt.Errorf("%w: max %d is below 1", ErrConfig, c.Max))
	}
	return errors.Join(problems...)
}

// Isolation is the constraint isolation. It rejects a move that puts more than Max
// replicas of one shard on the nodes with one value of a label. [NewIsolation]
// returns an Isolation, and the zero Isolation is not ready for use.
//
// Isolation counts the replicas of the shard on the nodes that the state describes,
// without the replica that a relocation moves and with the replica that the move
// places. For a move whose From equals its To, it counts the replicas as they are. A
// move that removes a replica passes, and so does a move to a node without the label
// or to a node that the state does not describe.
//
// # Concurrency
//
// An Isolation does not change after NewIsolation returns it, and it is safe for
// concurrent use.
//
// # Allocation contract
//
// Check does not allocate.
type Isolation struct {
	// label is the key of the label of nodes.
	label string
	// max is the most replicas of one shard under one value of the label.
	max int
}

var _ placement.Constraint = (*Isolation)(nil)

// NewIsolation returns the constraint isolation with the configuration cfg.
//
// Error modes:
//   - an error that wraps ErrConfig for each problem that cfg.Validate reports.
func NewIsolation(cfg IsolationConfig) (*Isolation, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Isolation{label: cfg.Label, max: cfg.Max}, nil
}

// Name returns IsolationName.
func (*Isolation) Name() shard.StrategyName {
	return IsolationName
}

// Check returns ErrIsolation when m puts more than Max replicas of its shard under the
// value of the label of its destination, and nil otherwise.
//
// # Allocation contract
//
// Zero alloc.
func (c *Isolation) Check(st *placement.State, m placement.Move) error {
	// A node that the state does not describe, and the empty node of a removal, have
	// no labels.
	node, _ := st.Node(m.To)
	value, has := node.Labels.Find(c.label)
	if !has {
		return nil
	}
	// The replica that m places counts once, and the replica on From does not count
	// again. For a move whose From equals its To, that is the replica on To.
	count := 1
	for _, r := range st.Replicas(m.Shard) {
		if r.Node == m.From {
			continue
		}
		// The state has no description of a draining or a gone node, so Node returns
		// a node without labels for it.
		host, _ := st.Node(r.Node)
		if label, has := host.Labels.Find(c.label); has && label.Value == value.Value {
			count++
		}
	}
	if count > c.max {
		return ErrIsolation
	}
	return nil
}
