// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package goal

import (
	"slices"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/shard"
)

// StickinessName is the registered name of the goal stickiness.
const StickinessName shard.StrategyName = "stickiness"

// Stickiness is the goal stickiness. Its cost counts the replicas that differ from the
// assignment at the start of the pass, which placement.State.Initial returns. A
// replica differs when its shard does not have an initial replica with the same node
// and role. Stickiness has no configuration, and the zero Stickiness is ready for use.
//
// A move back to the node and the role of an initial replica lowers the cost, so the
// cost of a pass that moves a replica away and back again is 0.
//
// # Concurrency
//
// A Stickiness is safe for concurrent use.
//
// # Allocation contract
//
// Delta does not allocate.
type Stickiness struct{}

var _ placement.Goal = Stickiness{}

// Name returns StickinessName.
func (Stickiness) Name() shard.StrategyName {
	return StickinessName
}

// Cost returns the number of replicas that differ from the initial replicas of their
// shards.
func (Stickiness) Cost(st *placement.State) float64 {
	cost := 0
	for _, sh := range st.Shards() {
		initial := st.Initial(sh)
		for _, r := range st.Replicas(sh) {
			if !slices.Contains(initial, r) {
				cost++
			}
		}
	}
	return float64(cost)
}

// Delta returns the change of the cost that m makes, without applying m.
//
// # Allocation contract
//
// Zero alloc.
func (Stickiness) Delta(st *placement.State, m placement.Move) float64 {
	// For a move whose From equals its To, the two terms cancel.
	initial := st.Initial(m.Shard)
	delta := 0.0
	if m.From != "" && !slices.Contains(initial, shard.Replica{Node: m.From, Role: m.Role}) {
		delta--
	}
	if m.To != "" && !slices.Contains(initial, shard.Replica{Node: m.To, Role: m.Role}) {
		delta++
	}
	return delta
}
