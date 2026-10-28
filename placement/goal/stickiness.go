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
//
//dokimi:subject placement.Goal new=newStickiness options=cells
type Stickiness struct{}

var _ placement.Goal = Stickiness{}

// Name returns StickinessName.
func (Stickiness) Name() shard.StrategyName {
	return StickinessName
}

// Cost returns the number of replicas that differ from the initial replicas of their
// shards. It returns 0 without reading the shards of a state to which Apply applied no
// move, because every replica of such a state is an initial replica.
func (Stickiness) Cost(st *placement.State) float64 {
	//dokimi:mutate-skip sbr-delete,ror-false: the loop counts no replica of a state without moves, so it returns the same 0
	if st.Moved() == 0 {
		return 0
	}
	cost := 0
	for i := range st.Shards() {
		initial := st.InitialAt(i)
		for _, r := range st.ReplicasAt(i) {
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
