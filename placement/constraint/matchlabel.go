// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package constraint

import (
	"fmt"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/shard"
)

// MatchLabelName is the registered name of the constraint match-label.
const MatchLabelName shard.StrategyName = "match-label"

// MatchLabelConfig is the configuration of the constraint match-label.
//
// The zero MatchLabelConfig is not valid, because it has no label.
type MatchLabelConfig struct {
	// Label is the key of the label that a shard and the nodes of its replicas share,
	// such as "jurisdiction".
	Label string `json:"label"`
}

// Validate returns nil when c is valid.
//
// Error modes:
//   - an error that wraps ErrConfig when Label is empty.
func (c MatchLabelConfig) Validate() error {
	if c.Label == "" {
		return fmt.Errorf("%w: label is empty", ErrConfig)
	}
	return nil
}

// MatchLabel is the constraint match-label. It rejects a replica on a node whose value
// of a label differs from the value of the label of its shard, or that does not have
// the label. It rejects every replica of a shard without the label, so such a shard
// has no feasible node, and its target has a deficit. [NewMatchLabel] returns a
// MatchLabel, and the zero MatchLabel is not ready for use.
//
// A move that removes a replica passes, and so does a move of a shard with the label
// to a node that the state does not describe.
//
// # Concurrency
//
// A MatchLabel does not change after NewMatchLabel returns it, and it is safe for
// concurrent use.
//
// # Allocation contract
//
// Check does not allocate.
type MatchLabel struct {
	// label is the key of the label.
	label string
}

var _ placement.Constraint = (*MatchLabel)(nil)

// NewMatchLabel returns the constraint match-label with the configuration cfg.
//
// Error modes:
//   - an error that wraps ErrConfig for each problem that cfg.Validate reports.
func NewMatchLabel(cfg MatchLabelConfig) (*MatchLabel, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &MatchLabel{label: cfg.Label}, nil
}

// Name returns MatchLabelName.
func (*MatchLabel) Name() shard.StrategyName {
	return MatchLabelName
}

// Check returns ErrMatchLabel when the shard of m does not have the label, or when the
// destination of m does not have the value of the shard, and nil otherwise.
//
// # Allocation contract
//
// Zero alloc.
func (c *MatchLabel) Check(st *placement.State, m placement.Move) error {
	if m.To == "" {
		return nil
	}
	sh, _ := st.Shard(m.Shard)
	want, has := sh.Labels.Find(c.label)
	if !has {
		return ErrMatchLabel
	}
	node, ok := st.Node(m.To)
	if !ok {
		return nil
	}
	if got, has := node.Labels.Find(c.label); !has || got.Value != want.Value {
		return ErrMatchLabel
	}
	return nil
}
