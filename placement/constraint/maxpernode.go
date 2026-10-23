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

// MaxPerNodeName is the registered name of the constraint max-per-node.
const MaxPerNodeName shard.StrategyName = "max-per-node"

// MaxPerNodeConfig is the configuration of the constraint max-per-node.
//
// The zero MaxPerNodeConfig is not valid, because its Max is 0.
type MaxPerNodeConfig struct {
	// Max is the most replicas of a node. It is at least one.
	Max int `json:"max"`
	// Role is the role of the replicas that Max counts, or shard.RoleNone for every
	// role.
	Role shard.Role `json:"role,omitempty"`
}

// Validate returns an error that joins every problem of c, or nil when c is valid.
// Each problem wraps ErrConfig.
//
// Error modes:
//   - an error that wraps ErrConfig when Max is below 1.
//   - an error that wraps ErrConfig when Role is not valid.
func (c MaxPerNodeConfig) Validate() error {
	var problems []error
	if c.Max < 1 {
		problems = append(problems, fmt.Errorf("%w: max %d is below 1", ErrConfig, c.Max))
	}
	if !c.Role.IsValid() {
		problems = append(problems, fmt.Errorf("%w: the role %v is not valid", ErrConfig, c.Role))
	}
	return errors.Join(problems...)
}

// MaxPerNode is the constraint max-per-node. It rejects a move that gives a node more
// than Max replicas in the role of the configuration, or more than Max replicas when
// the configuration has no role. [NewMaxPerNode] returns a MaxPerNode, and the zero
// MaxPerNode is not ready for use.
//
// MaxPerNode checks a move that relocates or adds a replica against the peak count of
// the destination with the shard: the shards that count on the node at the peak of the
// pass, which PeakCount and PeakRoleCount return. A shard keeps its replica on its source
// until its transition ends, and a transition in flight counts at its destination and at
// its fallback, so the moves of one plan and the transitions into a node share its room.
// A shard that counts on the destination already, such as a replica that returns to its
// node of the start of the pass, counts once. For a move whose From equals its To,
// MaxPerNode counts the replicas as they are. A move that removes a replica passes, and so
// does a move of a replica in another role than the role of the configuration.
//
// # Concurrency
//
// A MaxPerNode does not change after NewMaxPerNode returns it, and it is safe for
// concurrent use.
//
// # Allocation contract
//
// Check does not allocate.
type MaxPerNode struct {
	// max is the most replicas of a node.
	max int
	// role is the role of the replicas that max counts, or shard.RoleNone for every
	// role.
	role shard.Role
}

var _ placement.Constraint = (*MaxPerNode)(nil)

// NewMaxPerNode returns the constraint max-per-node with the configuration cfg.
//
// Error modes:
//   - an error that wraps ErrConfig for each problem that cfg.Validate reports.
func NewMaxPerNode(cfg MaxPerNodeConfig) (*MaxPerNode, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &MaxPerNode{max: cfg.Max, role: cfg.Role}, nil
}

// Name returns MaxPerNodeName.
func (*MaxPerNode) Name() shard.StrategyName {
	return MaxPerNodeName
}

// Check returns ErrMaxPerNode when m gives its destination more than Max replicas, and
// nil otherwise.
//
// # Allocation contract
//
// Zero alloc.
func (c *MaxPerNode) Check(st *placement.State, m placement.Move) error {
	if c.role != shard.RoleNone && m.Role != c.role {
		return nil
	}
	count := len(st.OnNode(m.To))
	if c.role != shard.RoleNone {
		count = st.RoleCount(m.To, c.role)
	}
	if m.From != m.To {
		// The empty node of a removal has no replicas, so a removal counts one replica,
		// and Max is at least one.
		count = st.PeakCount(m.To)
		if c.role != shard.RoleNone {
			count = st.PeakRoleCount(m.To, c.role)
		}
		initial := slices.ContainsFunc(st.Initial(m.Shard), func(r shard.Replica) bool {
			return r.Node == m.To && (c.role == shard.RoleNone || r.Role == c.role)
		})
		if !initial {
			count++
		}
	}
	if count > c.max {
		return ErrMaxPerNode
	}
	return nil
}
