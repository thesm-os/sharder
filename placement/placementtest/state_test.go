// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placementtest_test

import (
	"strconv"
	"sync"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/conform"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/tag"

	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/placementtest"
	"go.thesmos.sh/sharder/shard"
)

// The size of the cell of the suite of State.
const (
	// largeShards is the number of shards of the cell.
	largeShards = 10_000
	// largeNodes is the number of accepting nodes of the cell.
	largeNodes = 50
)

// The nodes of the cell of the suite of State that do not accept replicas.
const (
	// drainingNode drains.
	drainingNode shard.NodeID = "d0"
	// goneNode is gone.
	goneNode shard.NodeID = "g0"
	// idleNode is a node that Input.Draining lists as false.
	idleNode shard.NodeID = "x0"
	// unknownNode is a node that the cell does not have.
	unknownNode shard.NodeID = "zz"
)

// unknownMetric is a metric that the cell of the suite of State does not have.
const unknownMetric = "mem"

// The labels of the shards of the cell of the suite of State, and a label that no shard
// has.
var (
	tenant0      = tag.Tag{Key: placementtest.Tenant, Value: "t0"}
	tenant1      = tag.Tag{Key: placementtest.Tenant, Value: "t1"}
	homeA        = tag.Tag{Key: placementtest.Home, Value: "a"}
	unknownLabel = tag.Tag{Key: placementtest.Home, Value: "b"}
)

// unknownShard is a shard that the layout of the cell of the suite of State does not
// list.
var unknownShard = shard.ID{ID128: id.ID128{0: 0xff}}

// largeInput returns the input of the cell whose state the suite of State measures:
// largeShards shards under placementtest.Replicated on largeNodes accepting nodes,
// drainingNode and goneNode. It builds the input on its first call, and every call
// returns that input. NewState does not modify it.
//
//   - Accepting node i is in zone a, b or c by i modulo 3. It declares the capacity
//     placementtest.CPU, and placementtest.Disk unless i modulo 4 is 3.
//   - Shard i has the ID whose last four bytes are i+1. It has the label tenant0 or
//     tenant1 by i modulo 2 unless i modulo 3 is 0. It has the label homeA when i
//     modulo 5 is 0.
//   - Shard i has no load when i modulo 11 is 0, so it has the mean load. It has no
//     placement when i modulo 17 is 0.
//   - Shard i has its primary on node i modulo largeNodes and its secondaries on the next
//     two nodes. Its second secondary is on drainingNode when i modulo 23 is 0, and
//     otherwise on goneNode when i modulo 29 is 0.
//   - A placed shard i has a transition in flight of its primary when i modulo 31 is 0.
//     The transition goes to the node after its secondaries, with the node of its
//     primary as the fallback.
var largeInput = sync.OnceValue(func() *placement.Input {
	in := &placement.Input{
		Transitions: make(map[shard.ID]placement.InFlight),
		Draining:    map[shard.NodeID]bool{drainingNode: true, idleNode: false},
		ShardLoad:   make(map[shard.ID]shard.Load, largeShards),
		Model:       placementtest.Replicated,
		Layout:      partition.Layout{Shards: make([]partition.Shard, largeShards)},
		Current:     shard.Assignment{Placements: make(map[shard.ID]shard.Placement, largeShards)},
		Nodes:       make([]shard.Node, largeNodes),
		Metrics: []placement.Metric{
			{Name: placementtest.CPU},
			{Name: placementtest.Bytes, Capacity: placementtest.Disk},
		},
	}
	for i := range largeNodes {
		capacity := map[string]float64{placementtest.CPU: 1_000}
		if i%4 != 3 {
			capacity[placementtest.Disk] = 10_000
		}
		in.Nodes[i] = shard.Node{
			ID:       shard.NodeID("n" + strconv.Itoa(i)),
			Labels:   tag.Tags{{Key: placementtest.Zone, Value: string(rune('a' + i%3))}},
			Capacity: capacity,
		}
	}
	for i := range largeShards {
		seq := i + 1
		sh := shard.ID{ID128: id.ID128{12: byte(seq >> 24), 13: byte(seq >> 16), 14: byte(seq >> 8), 15: byte(seq)}}
		var labels tag.Tags
		if i%3 != 0 {
			tenant := tenant0
			if i%2 == 1 {
				tenant = tenant1
			}
			labels = append(labels, tenant)
		}
		if i%5 == 0 {
			labels = append(labels, homeA)
		}
		in.Layout.Shards[i] = partition.Shard{Labels: labels, ID: sh}
		if i%11 != 0 {
			in.ShardLoad[sh] = shard.Load{placementtest.CPU: float64(i % 7), placementtest.Bytes: float64(i % 13)}
		}
		if i%17 == 0 {
			continue
		}
		primary := in.Nodes[i%largeNodes].ID
		last := in.Nodes[(i+2)%largeNodes].ID
		switch {
		case i%23 == 0:
			last = drainingNode
		case i%29 == 0:
			last = goneNode
		}
		in.Current.Placements[sh] = shard.Placement{Shard: sh, Replicas: []shard.Replica{
			{Node: primary, Role: shard.RolePrimary},
			{Node: in.Nodes[(i+1)%largeNodes].ID, Role: shard.RoleSecondary},
			{Node: last, Role: shard.RoleSecondary},
		}}
		if i%31 == 0 {
			in.Transitions[sh] = placement.InFlight{
				To:       in.Nodes[(i+3)%largeNodes].ID,
				Fallback: primary,
				Role:     shard.RolePrimary,
			}
		}
	}
	return in
})

// newLargeState returns a new state of largeInput, the subject of the suite of State.
// Each check applies its moves to a state of its own.
func newLargeState(h *placementtest.StateHandle) *placement.State {
	st, err := placement.NewState(largeInput())
	assert.NoError(h, err, "NewState must accept the cell of the suite of State")
	return st
}

// stateOptions returns the options of the application of the suite of State to the
// states of newLargeState. The generated calls draw the shards, the nodes, the metrics
// and the labels of the cell, and one of each kind that the cell does not have.
func stateOptions() []conform.Option[placementtest.StateMethods] {
	in := largeInput()
	shards := make([]shard.ID, 0, largeShards+1)
	for _, sh := range in.Layout.Shards {
		shards = append(shards, sh.ID)
	}
	nodes := make([]shard.NodeID, 0, largeNodes+5)
	for _, n := range in.Nodes {
		nodes = append(nodes, n.ID)
	}
	return []conform.Option[placementtest.StateMethods]{
		conform.Using[placementtest.StateMethods](prop.SampledFrom(append(shards, unknownShard)...)),
		conform.Using[placementtest.StateMethods](prop.SampledFrom(
			append(nodes, drainingNode, goneNode, idleNode, unknownNode, "")...)),
		conform.Using[placementtest.StateMethods](prop.SampledFrom(placementtest.CPU, placementtest.Bytes,
			unknownMetric)),
		conform.Using[placementtest.StateMethods](prop.SampledFrom(tenant0, tenant1, homeA, unknownLabel)),
	}
}
