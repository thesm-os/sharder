// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package goal

import (
	"fmt"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/shard"
)

// SpreadName is the registered name of the goal spread.
const SpreadName shard.StrategyName = "spread"

// SpreadConfig is the configuration of the goal spread.
//
// The zero SpreadConfig is not valid, because it has no label.
type SpreadConfig struct {
	// Label is the key of the label of nodes whose values spread the replicas of a
	// shard, such as "zone".
	Label string `json:"label"`
}

// Validate returns nil when c is valid.
//
// Error modes:
//   - an error that wraps ErrConfig when Label is empty.
func (c SpreadConfig) Validate() error {
	if c.Label == "" {
		return fmt.Errorf("%w: label is empty", ErrConfig)
	}
	return nil
}

// Spread is the goal spread. Its cost counts the replicas of each shard that share a
// value of a label with an earlier replica of the shard, so a shard with k replicas
// under one value adds k-1 to the cost. A replica on a node without the label adds 0,
// and so does a replica on a node that the state does not describe. [NewSpread]
// returns a Spread, and the zero Spread is not ready for use.
//
// Delta counts the shared values of the one shard of a move before and after it, so
// it reads only the nodes of the replicas of that shard.
//
// # Concurrency
//
// A Spread does not change after NewSpread returns it, and it is safe for concurrent
// use.
//
// # Allocation contract
//
// Delta does not allocate.
type Spread struct {
	// label is the key of the label of nodes.
	label string
}

var _ placement.Goal = (*Spread)(nil)

// NewSpread returns the goal spread with the configuration cfg.
//
// Error modes:
//   - an error that wraps ErrConfig for each problem that cfg.Validate reports.
func NewSpread(cfg SpreadConfig) (*Spread, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Spread{label: cfg.Label}, nil
}

// Name returns SpreadName.
func (*Spread) Name() shard.StrategyName {
	return SpreadName
}

// Cost returns the number of replicas that share a value of the label with an earlier
// replica of their shard.
func (g *Spread) Cost(st *placement.State) float64 {
	cost := 0
	for _, sh := range st.Shards() {
		cost += g.shared(st, st.Replicas(sh), "", "")
	}
	return float64(cost)
}

// Delta returns the change of the cost that m makes, without applying m.
//
// # Allocation contract
//
// Zero alloc.
func (g *Spread) Delta(st *placement.State, m placement.Move) float64 {
	if m.From == m.To {
		return 0
	}
	replicas := st.Replicas(m.Shard)
	return float64(g.shared(st, replicas, m.From, m.To) - g.shared(st, replicas, "", ""))
}

// shared returns the number of replicas that share a value of the label with an
// earlier replica, among replicas without the replica on skip and with a replica on
// extra. An empty skip does not remove a replica, and an empty extra does not add one.
func (g *Spread) shared(st *placement.State, replicas []shard.Replica, skip, extra shard.NodeID) int {
	count := 0
	for i, r := range replicas {
		if r.Node == skip {
			continue
		}
		value, ok := g.value(st, r.Node)
		if !ok {
			continue
		}
		for _, earlier := range replicas[:i] {
			if v, ok := g.value(st, earlier.Node); ok && earlier.Node != skip && v == value {
				count++
				break
			}
		}
	}
	// The state does not describe the empty node, so an empty extra has no value.
	value, ok := g.value(st, extra)
	if !ok {
		return count
	}
	for _, r := range replicas {
		if v, ok := g.value(st, r.Node); ok && r.Node != skip && v == value {
			return count + 1
		}
	}
	return count
}

// value returns the value of the label of node n, and reports whether the state
// describes n and n has the label.
func (g *Spread) value(st *placement.State, n shard.NodeID) (string, bool) {
	// Node returns a node without labels for a node that the state does not describe.
	node, _ := st.Node(n)
	label, ok := node.Labels.Find(g.label)
	return label.Value, ok
}
