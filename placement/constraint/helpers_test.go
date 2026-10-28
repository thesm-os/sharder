// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package constraint_test

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
	// n1 is in zone a and region eu, with the jurisdiction de and the capacity cpu 10.
	n1 shard.NodeID = "n1"
	// n2 is in zone b and region eu, with the jurisdiction de and the capacity cpu 10.
	n2 shard.NodeID = "n2"
	// n3 is in zone a and region us, with the jurisdiction us and the capacity cpu 4.
	n3 shard.NodeID = "n3"
	// n4 has no labels and no capacity.
	n4 shard.NodeID = "n4"
	// d1 drains.
	d1 shard.NodeID = "d1"
)

// The keys and values of the labels of the cell of the tests of this package.
const (
	region       = "region"
	jurisdiction = "jurisdiction"
	zoneA        = "a"
	zoneB        = "b"
	eu           = "eu"
	us           = "us"
	de           = "de"
)

// The shards of the cell of the tests of this package, in the order of their IDs. The
// layout does not list s9.
var (
	s1 = shard.ID{ID128: id.ID128{15: 1}}
	s2 = shard.ID{ID128: id.ID128{15: 2}}
	s3 = shard.ID{ID128: id.ID128{15: 3}}
	s4 = shard.ID{ID128: id.ID128{15: 4}}
	s5 = shard.ID{ID128: id.ID128{15: 5}}
	s6 = shard.ID{ID128: id.ID128{15: 6}}
	s9 = shard.ID{ID128: id.ID128{15: 9}}
)

// cell returns the input of the cell of the tests of this package. Its model is
// placementtest.Replicated, and its one metric is cpu.
//
//   - s1 has the jurisdiction de, its primary on n1, its secondary on n2, and a load
//     of 3.
//   - s2 has the jurisdiction us, its primary on n3, and a load of 3.
//   - s3 has its primary on the draining node d1, and a load of 1.
//   - s4 has no placement and no load, so its load is the mean, 2.25.
//   - s5 has its primary on n1, a transition in flight of its primary to n2 with the
//     fallback n1, and a load of 2.
//   - s6 has its primary on n1 and its secondary on n3, both in zone a, and a load
//     of 0.
func cell() *placement.Input {
	node := func(id shard.NodeID, zone, reg, law string, cpu float64) shard.Node {
		return shard.Node{
			ID: id,
			Labels: tag.Tags{
				{Key: placementtest.Zone, Value: zone},
				{Key: region, Value: reg},
				{Key: jurisdiction, Value: law},
			},
			Capacity: map[string]float64{placementtest.CPU: cpu},
		}
	}
	return &placement.Input{
		Transitions: map[shard.ID]placement.InFlight{s5: {To: n2, Fallback: n1, Role: shard.RolePrimary}},
		Draining:    map[shard.NodeID]bool{d1: true},
		ShardLoad: map[shard.ID]shard.Load{
			s1: {placementtest.CPU: 3},
			s2: {placementtest.CPU: 3},
			s3: {placementtest.CPU: 1},
			s5: {placementtest.CPU: 2},
			s6: {placementtest.CPU: 0},
		},
		Model: placementtest.Replicated,
		Layout: partition.Layout{Shards: []partition.Shard{
			{Labels: tag.Tags{{Key: jurisdiction, Value: de}}, ID: s1},
			{Labels: tag.Tags{{Key: jurisdiction, Value: us}}, ID: s2},
			{ID: s3},
			{ID: s4},
			{ID: s5},
			{ID: s6},
		}},
		Current: shard.Assignment{Placements: map[shard.ID]shard.Placement{
			s1: {Shard: s1, Replicas: []shard.Replica{
				{Node: n1, Role: shard.RolePrimary},
				{Node: n2, Role: shard.RoleSecondary},
			}},
			s2: {Shard: s2, Replicas: []shard.Replica{{Node: n3, Role: shard.RolePrimary}}},
			s3: {Shard: s3, Replicas: []shard.Replica{{Node: d1, Role: shard.RolePrimary}}},
			s5: {Shard: s5, Replicas: []shard.Replica{{Node: n1, Role: shard.RolePrimary}}},
			s6: {Shard: s6, Replicas: []shard.Replica{
				{Node: n1, Role: shard.RolePrimary},
				{Node: n3, Role: shard.RoleSecondary},
			}},
		}},
		Nodes: []shard.Node{
			node(n1, zoneA, eu, de, 10),
			node(n2, zoneB, eu, de, 10),
			node(n3, zoneA, us, us, 4),
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

// cells returns the options of the suite of placement.Constraint that draw the states and
// the moves of the calls of Check from the cells of placementtest.Inputs. The directive
// subject of each constraint passes them to its generated tests.
func cells() []placementtest.ConstraintOption {
	return placementtest.ConstraintCells
}
