// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package constraint

import (
	"fmt"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/shard"
)

// CapacityName is the registered name of the constraint capacity.
const CapacityName shard.StrategyName = "capacity"

// precision is the relative amount by which the load of a node may exceed a capacity,
// less the headroom, and still fit. The State adds and subtracts the load of each move in
// the order of the moves, so two orders of the same moves can give loads that differ in
// their last bits. The planner and Validate apply the moves of a plan in different orders.
// A load within a billionth of the limit fits, so such a rounding error does not decide a
// check.
const precision = 1e-9

// CapacityConfig is the configuration of the constraint capacity.
//
// The zero CapacityConfig does not keep capacity free.
type CapacityConfig struct {
	// Headroom is the fraction of each capacity that the constraint keeps free, at
	// least 0 and below 1.
	Headroom float64 `json:"headroom"`
}

// Validate returns nil when c is valid.
//
// Error modes:
//   - an error that wraps ErrConfig when Headroom is NaN, below 0, or not below 1.
func (c CapacityConfig) Validate() error {
	if !(c.Headroom >= 0 && c.Headroom < 1) {
		return fmt.Errorf("%w: headroom %v is not from 0 to below 1", ErrConfig, c.Headroom)
	}
	return nil
}

// Capacity is the constraint capacity. It rejects a move that puts the load of a node
// on a metric above the capacity that the metric fills, less the headroom.
// [NewCapacity] returns a Capacity. The zero Capacity does not keep capacity free.
//
// Capacity checks a move that relocates or adds a replica against the peak load of
// the destination with the shard. PeakLoad and PeakCounts return that load. A shard
// keeps its load on its source until its transition ends, so the moves of one plan
// into a node share its free capacity. Capacity checks a move whose From equals its To
// against NodeLoad of the node, which is the load once the moves end. A node above its
// capacity then keeps the replicas that fit as the planner moves the others off it.
//
// Capacity passes a move that removes a replica and a move to a node that the state
// does not describe. It does not check a capacity that the node does not declare. A load
// that exceeds a limit by at most a billionth of the limit fits, and so does a load that
// exceeds a limit below 1 by at most a billionth.
//
// # Concurrency
//
// A Capacity does not change after NewCapacity returns it, and it is safe for
// concurrent use.
//
// # Allocation contract
//
// Check does not allocate.
//
//dokimi:subject placement.Constraint new=capacitySubject options=cells
type Capacity struct {
	// headroom is the fraction of each capacity that the constraint keeps free.
	headroom float64
}

var _ placement.Constraint = (*Capacity)(nil)

// NewCapacity returns the constraint capacity with the configuration cfg.
//
// Error modes:
//   - an error that wraps ErrConfig for each problem that cfg.Validate reports.
func NewCapacity(cfg CapacityConfig) (*Capacity, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Capacity{headroom: cfg.Headroom}, nil
}

// Name returns CapacityName.
func (*Capacity) Name() shard.StrategyName {
	return CapacityName
}

// Check returns ErrCapacity when m puts its destination above a capacity, less the
// headroom, and nil otherwise.
//
// # Allocation contract
//
// Zero alloc.
func (c *Capacity) Check(st *placement.State, m placement.Move) error {
	// A node that the state does not describe, and the empty node of a removal, have
	// no capacities, so the loop below passes them.
	node, _ := st.Node(m.To)
	load := st.PeakLoad(m.To)
	if m.From == m.To {
		load = st.NodeLoad(m.To)
	}
	// A move whose From equals its To keeps a replica that PeakLoad counts.
	added := !st.PeakCounts(m.To, m.Shard)
	shardLoad := st.ShardLoad(m.Shard)
	for _, metric := range st.Metrics() {
		capacity, ok := node.Capacity[metric.Capacity]
		if !ok {
			continue
		}
		total := load[metric.Name]
		if added {
			total += shardLoad[metric.Name]
		}
		limit := capacity * (1 - c.headroom)
		//dokimi:mutate-skip ror-boundary: precision absorbs rounding error, so a load of exactly the limit and the precision is noise
		if total > limit+precision*max(1, limit) {
			return ErrCapacity
		}
	}
	return nil
}
