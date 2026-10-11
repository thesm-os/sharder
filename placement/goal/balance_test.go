// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package goal_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/tag"

	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/goal"
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
	s1 = shard.ID{ID: id.New128([16]byte{15: 1})}
	s2 = shard.ID{ID: id.New128([16]byte{15: 2})}
	s3 = shard.ID{ID: id.New128([16]byte{15: 3})}
	s4 = shard.ID{ID: id.New128([16]byte{15: 4})}
	s5 = shard.ID{ID: id.New128([16]byte{15: 5})}
	s6 = shard.ID{ID: id.New128([16]byte{15: 6})}
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

// TestBalance checks the validation of a BalanceConfig, NewBalance, and the cost and
// the deltas of a Balance.
func TestBalance(t *testing.T) {
	t.Parallel()

	t.Run("BalanceConfig", func(t *testing.T) {
		t.Parallel()

		t.Run("Validate", func(t *testing.T) {
			t.Parallel()

			t.Run("returns nil for a metric", func(t *testing.T) {
				t.Parallel()
				assert.NoError(t, goal.BalanceConfig{Metric: placementtest.CPU}.Validate(),
					"Validate must accept the configuration")
			})

			t.Run("returns ErrConfig for an empty metric", func(t *testing.T) {
				t.Parallel()
				err := goal.BalanceConfig{}.Validate()
				expect.ErrorIs(t, err, goal.ErrConfig, "Validate must return ErrConfig")
				expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of Validate must classify as Invalid")
			})
		})
	})

	t.Run("NewBalance", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrConfig for a configuration that Validate refuses", func(t *testing.T) {
			t.Parallel()
			_, err := goal.NewBalance(goal.BalanceConfig{})
			assert.ErrorIs(t, err, goal.ErrConfig, "NewBalance must refuse an empty metric")
		})
	})

	t.Run("Name", func(t *testing.T) {
		t.Parallel()

		t.Run("returns BalanceName", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, newBalance(t).Name(), goal.BalanceName, "Name must return the registered name")
		})
	})

	t.Run("Cost", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the variance of the utilisation of the nodes with a capacity", func(t *testing.T) {
			t.Parallel()
			assert.CloseTo(t, newBalance(t).Cost(newState(t, cell())), 0.46/3-1.0/9, tolerance,
				"Cost must return the variance of 0.3, 0.6 and 0.1")
		})

		t.Run("returns 0 for a cell without a capacity", func(t *testing.T) {
			t.Parallel()
			in := cell()
			in.Nodes = in.Nodes[3:]
			assert.Equal(t, newBalance(t).Cost(newState(t, in)), 0.0, "Cost must return 0 without a capacity")
		})
	})

	t.Run("Delta", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			give placement.Move
			want float64
		}{
			{
				name: "returns the change of the variance for a move between two nodes",
				give: placement.Move{From: n2, To: n3, Shard: s2, Role: shard.RolePrimary},
				want: -0.04,
			},
			{
				name: "returns the change of the variance for a move to a node without a capacity",
				give: placement.Move{From: n1, To: n4, Shard: s3, Role: shard.RolePrimary},
				want: (0.41/3 - 0.81/9) - (0.46/3 - 1.0/9),
			},
			{
				name: "returns the change of the variance for a move from a draining node",
				give: placement.Move{From: d1, To: n3, Shard: s4, Role: shard.RolePrimary},
				want: (0.5125/3 - 1.3225/9) - (0.46/3 - 1.0/9),
			},
			{
				name: "returns 0 for a move whose From equals its To",
				give: placement.Move{From: n1, To: n1, Shard: s3, Role: shard.RolePrimary},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.CloseTo(t, newBalance(t).Delta(newState(t, cell()), tt.give), tt.want, tolerance,
					"Delta must return the change of the variance")
			})
		}

		t.Run("returns 0 for a cell without a capacity", func(t *testing.T) {
			t.Parallel()
			in := cell()
			in.Nodes = in.Nodes[3:]
			assert.Equal(t, newBalance(t).Delta(newState(t, in), placement.Move{
				From: g1, To: n4, Shard: s5,
				Role: shard.RolePrimary,
			}), 0.0, "Delta must return 0 without a capacity")
		})
	})
}

// TestBalanceConformance checks the laws of placement.Goal against a Balance.
func TestBalanceConformance(t *testing.T) {
	t.Parallel()
	placementtest.AssertGoal(t, newBalance(t))
}

// TestBalanceAllocs checks that Delta of a Balance does not allocate.
func TestBalanceAllocs(t *testing.T) {
	placementtest.AssertGoalAllocs(t, newBalance(t))
}

// BenchmarkBalance reports the cost of Delta of a Balance, and fails when it
// allocates.
func BenchmarkBalance(b *testing.B) {
	b.Run("Delta", func(b *testing.B) {
		g := newBalance(b)
		st := newState(b, cell())
		m := placement.Move{From: n2, To: n3, Shard: s2, Role: shard.RolePrimary}
		var got float64

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			got = g.Delta(st, m)
		}

		assert.CloseTo(b, got, -0.04, tolerance, "the benchmark must measure a move that lowers the variance")
	})
}

// newState returns the state of in, and stops tb when NewState refuses in.
func newState(tb assert.TB, in *placement.Input) *placement.State {
	tb.Helper()
	st, err := placement.NewState(in)
	assert.NoError(tb, err, "NewState must accept the input of the test")
	return st
}

// newBalance returns the goal balance of the metric cpu, and stops tb when NewBalance
// refuses it.
func newBalance(tb assert.TB) *goal.Balance {
	tb.Helper()
	g, err := goal.NewBalance(goal.BalanceConfig{Metric: placementtest.CPU})
	assert.NoError(tb, err, "NewBalance must accept the metric cpu")
	return g
}
