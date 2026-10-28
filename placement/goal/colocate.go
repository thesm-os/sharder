// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package goal

import (
	"fmt"

	"go.thesmos.sh/core/tag"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/shard"
)

// ColocateName is the registered name of the goal colocate.
const ColocateName shard.StrategyName = "colocate"

// ColocateConfig is the configuration of the goal colocate.
//
// The zero ColocateConfig is not valid, because it has no group.
type ColocateConfig struct {
	// Group is the key of the label of shards whose value is the group of a shard, such
	// as "tenant".
	Group string `json:"group"`
}

// Validate returns nil when c is valid.
//
// Error modes:
//   - an error that wraps ErrConfig when Group is empty.
func (c ColocateConfig) Validate() error {
	if c.Group == "" {
		return fmt.Errorf("%w: group is empty", ErrConfig)
	}
	return nil
}

// Colocate is the goal colocate. Its cost is the sum over the groups of the nodes of
// each group beyond placement.State.Desired, the number of replicas of one shard. The
// nodes of a group are the nodes with a replica of a shard of the group. A group with
// no more nodes than Desired adds 0 to the cost, so a group whose shards share their
// nodes adds 0. A shard without the label Group is not in a group. [NewColocate]
// returns a Colocate, and the zero Colocate is not ready for use.
//
// Delta reads the counts of placement.State.Labelled and LabelledNodes for the group
// of the shard of a move, so it does not visit the other shards of the group.
//
// # Concurrency
//
// A Colocate does not change after NewColocate returns it, and it is safe for
// concurrent use.
//
// # Allocation contract
//
// Delta does not allocate. Cost allocates the set of the groups.
//
//dokimi:subject placement.Goal new=colocateSubject options=cells
type Colocate struct {
	// group is the key of the label of shards.
	group string
}

var _ placement.Goal = (*Colocate)(nil)

// NewColocate returns the goal colocate with the configuration cfg.
//
// Error modes:
//   - an error that wraps ErrConfig for each problem that cfg.Validate reports.
func NewColocate(cfg ColocateConfig) (*Colocate, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Colocate{group: cfg.Group}, nil
}

// Name returns ColocateName.
func (*Colocate) Name() shard.StrategyName {
	return ColocateName
}

// Cost returns the sum over the groups of the nodes of each group beyond Desired.
func (g *Colocate) Cost(st *placement.State) float64 {
	groups := map[tag.Tag]bool{}
	cost := 0
	for _, sh := range st.Shards() {
		s, _ := st.Shard(sh)
		label, ok := s.Labels.Find(g.group)
		//dokimi:mutate-skip lcr-right: a shard without the label reads the zero tag. The state counts that tag on no node, so its group adds 0 to the cost
		if !ok || groups[label] {
			continue
		}
		groups[label] = true
		cost += max(0, st.LabelledNodes(label)-st.Desired())
	}
	return float64(cost)
}

// Delta returns the change of the cost that m makes, without applying m.
//
// # Allocation contract
//
// Zero alloc.
func (g *Colocate) Delta(st *placement.State, m placement.Move) float64 {
	s, _ := st.Shard(m.Shard)
	label, ok := s.Labels.Find(g.group)
	//dokimi:mutate-skip lcr-left: a shard without the label reads the zero tag. The state counts that tag on no node, so m leaves it on one node at most, and Desired is at least 1
	if m.From == m.To || !ok {
		return 0
	}
	before := st.LabelledNodes(label)
	after := before
	// The state does not count a label on the empty node of an addition.
	if st.Labelled(label, m.From) == 1 {
		after--
	}
	if m.To != "" && st.Labelled(label, m.To) == 0 {
		after++
	}
	return float64(max(0, after-st.Desired()) - max(0, before-st.Desired()))
}
