// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package goal_test

import (
	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/tag"

	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/placementtest"
	"go.thesmos.sh/sharder/shard"
)

// The nodes of the cell of the tests of this package.
const (
	// n1 is in zone a and region eu, with the capacity cpu 10.
	n1 shard.NodeID = "n1"
	// n2 is in zone b and region eu, with the capacity cpu 10.
	n2 shard.NodeID = "n2"
	// n3 is in zone a and region us, with the capacity cpu 20.
	n3 shard.NodeID = "n3"
	// n4 has no labels and no capacity.
	n4 shard.NodeID = "n4"
	// d1 drains.
	d1 shard.NodeID = "d1"
	// g1 is gone.
	g1 shard.NodeID = "g1"
)

// The keys and values of the labels of the cell of the tests of this package.
const (
	region = "region"
	eu     = "eu"
	us     = "us"
	zoneA  = "a"
	zoneB  = "b"
)

// tolerance is the absolute error that the tests allow in a cost or a delta.
const tolerance = 1e-12

// The shards of the cell of the tests of this package, in the order of their IDs.
var (
	s1 = shard.ID{ID128: id.ID128{15: 1}}
	s2 = shard.ID{ID128: id.ID128{15: 2}}
	s3 = shard.ID{ID128: id.ID128{15: 3}}
	s4 = shard.ID{ID128: id.ID128{15: 4}}
	s5 = shard.ID{ID128: id.ID128{15: 5}}
	s6 = shard.ID{ID128: id.ID128{15: 6}}
)

// The labels of the shards of the cell of the tests of this package.
var (
	t0     = tag.Tag{Key: placementtest.Tenant, Value: "t0"}
	t1     = tag.Tag{Key: placementtest.Tenant, Value: "t1"}
	homeEU = tag.Tag{Key: placementtest.Home, Value: eu}
	homeUS = tag.Tag{Key: placementtest.Home, Value: us}
)

// cell returns the input of the cell of the tests of this package. Its model is
// placementtest.Replicated, and its one metric is cpu.
//
//   - s1 has the labels t0 and homeEU, its primary on n1, its secondaries on n2 and
//     n3, and a load of 2.
//   - s2 has the label t0, its primary on n2, and a load of 4.
//   - s3 has the labels t1 and homeUS, its primary on n1, and a load of 1.
//   - s4 has its primary on the draining node d1, and a load of 3.
//   - s5 has the label t1, its primary on the gone node g1, and the mean load, 2.5.
//   - s6 has no placement and the mean load.
//
// The utilisations of n1, n2 and n3 are 0.3, 0.6 and 0.1. Their replica counts, with
// n4, are 2, 2, 1 and 0.
func cell() *placement.Input {
	node := func(id shard.NodeID, zone, reg string, cpu float64) shard.Node {
		return shard.Node{
			ID:       id,
			Labels:   tag.Tags{{Key: placementtest.Zone, Value: zone}, {Key: region, Value: reg}},
			Capacity: map[string]float64{placementtest.CPU: cpu},
		}
	}
	return &placement.Input{
		Draining: map[shard.NodeID]bool{d1: true},
		ShardLoad: map[shard.ID]shard.Load{
			s1: {placementtest.CPU: 2},
			s2: {placementtest.CPU: 4},
			s3: {placementtest.CPU: 1},
			s4: {placementtest.CPU: 3},
		},
		Model: placementtest.Replicated,
		Layout: partition.Layout{Shards: []partition.Shard{
			{Labels: tag.Tags{t0, homeEU}, ID: s1},
			{Labels: tag.Tags{t0}, ID: s2},
			{Labels: tag.Tags{t1, homeUS}, ID: s3},
			{ID: s4},
			{Labels: tag.Tags{t1}, ID: s5},
			{ID: s6},
		}},
		Current: shard.Assignment{Placements: map[shard.ID]shard.Placement{
			s1: {Shard: s1, Replicas: []shard.Replica{
				{Node: n1, Role: shard.RolePrimary},
				{Node: n2, Role: shard.RoleSecondary},
				{Node: n3, Role: shard.RoleSecondary},
			}},
			s2: {Shard: s2, Replicas: []shard.Replica{{Node: n2, Role: shard.RolePrimary}}},
			s3: {Shard: s3, Replicas: []shard.Replica{{Node: n1, Role: shard.RolePrimary}}},
			s4: {Shard: s4, Replicas: []shard.Replica{{Node: d1, Role: shard.RolePrimary}}},
			s5: {Shard: s5, Replicas: []shard.Replica{{Node: g1, Role: shard.RolePrimary}}},
		}},
		Nodes: []shard.Node{
			node(n1, zoneA, eu, 10),
			node(n2, zoneB, eu, 10),
			node(n3, zoneA, us, 20),
			{ID: n4},
		},
		Metrics:     []placement.Metric{{Name: placementtest.CPU}},
		Parallelism: 1,
	}
}

// newState returns the state of in, and stops tb when NewState refuses in.
func newState(tb assert.TB, in *placement.Input) *placement.State {
	tb.Helper()
	st, err := placement.NewState(in)
	assert.NoError(tb, err, "NewState must accept the input of the test")
	return st
}

// cells returns the options of the suite of placement.Goal that draw the states and the
// moves of the calls of Cost and Delta from the cells of placementtest.Inputs. The
// directive subject of each goal passes them to its generated tests.
func cells() []placementtest.GoalOption {
	return placementtest.GoalCells
}
