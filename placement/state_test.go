// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placement_test

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/assert/stateful"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/tag"

	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/placementtest"
	"go.thesmos.sh/sharder/replication"
	"go.thesmos.sh/sharder/shard"
)

// The nodes of the cell of the tests.
const (
	// n1 is accepting, in zone a, with the capacities cpu 10 and disk 100.
	n1 shard.NodeID = "n1"
	// n2 is accepting, in zone b, with the capacities cpu 10 and disk 100.
	n2 shard.NodeID = "n2"
	// n3 is accepting, in zone a, with the capacity cpu 20 and no disk.
	n3 shard.NodeID = "n3"
	// d1 drains.
	d1 shard.NodeID = "d1"
	// x1 is a node that Input.Draining lists as not draining.
	x1 shard.NodeID = "x1"
	// g1 is gone, and hosts the primary of s3.
	g1 shard.NodeID = "g1"
	// unknown is a node that the cell does not have.
	unknown shard.NodeID = "zz"
)

// The names of the cell of the tests.
const (
	// cellID is the cell of the tests.
	cellID shard.CellID = "c1"
	// mem is a metric that the cell does not have.
	mem = "mem"
	// zoneA is the value a of the label zone.
	zoneA = "a"
	// zoneB is the value b of the label zone.
	zoneB = "b"
)

// The labels of the values that the properties generate.
const (
	labelInput = "input"
	labelMove  = "move"
)

// s1 to s5 are the shards of the cell of the tests, in the order of their IDs. s8 and
// s9 are shards that the layout does not list.
var (
	s1 = shard.ID{ID: id.New128([16]byte{15: 1})}
	s2 = shard.ID{ID: id.New128([16]byte{15: 2})}
	s3 = shard.ID{ID: id.New128([16]byte{15: 3})}
	s4 = shard.ID{ID: id.New128([16]byte{15: 4})}
	s5 = shard.ID{ID: id.New128([16]byte{15: 5})}
	s8 = shard.ID{ID: id.New128([16]byte{15: 8})}
	s9 = shard.ID{ID: id.New128([16]byte{15: 9})}
)

// The labels of the shards of the cell of the tests.
var (
	t0    = tag.Tag{Key: placementtest.Tenant, Value: "t0"}
	t1    = tag.Tag{Key: placementtest.Tenant, Value: "t1"}
	homeA = tag.Tag{Key: placementtest.Home, Value: zoneA}
)

// cell returns the input of the cell of the tests. Its model is
// placementtest.Replicated, so each shard has one primary and two secondaries.
//
//   - s1 has the label t0, its primary on n1, and its secondaries on n2 and d1.
//   - s2 has the labels t0 and homeA, its primary on n2, and a secondary on n3, which
//     has a fault of s2.
//   - s3 has its primary on the gone node g1, and a load without bytes.
//   - s4 has the label t1, and no placement and no load.
//   - s5 has its primary on n3, and a transition in flight to n1 with the fallback n3.
//
// The layout lists the shards in the order s3, s1, s5, s2, s4, and Input.Nodes lists
// n3 before n1 and n2, so NewState orders both.
func cell() *placement.Input {
	return &placement.Input{
		Transitions: map[shard.ID]placement.InFlight{s5: {To: n1, Fallback: n3}},
		Faults:      map[shard.ID][]shard.NodeID{s2: {n3}},
		Draining:    map[shard.NodeID]bool{d1: true, x1: false},
		ShardLoad: map[shard.ID]shard.Load{
			s1: {placementtest.CPU: 2, placementtest.Bytes: 10},
			s2: {placementtest.CPU: 4, placementtest.Bytes: 30},
			s3: {placementtest.CPU: 1},
			s5: {placementtest.CPU: 3, placementtest.Bytes: 5},
		},
		Model: placementtest.Replicated,
		Layout: partition.Layout{Cell: cellID, Shards: []partition.Shard{
			{ID: s3},
			{Labels: tag.Tags{t0}, ID: s1},
			{ID: s5},
			{Labels: tag.Tags{t0, homeA}, ID: s2},
			{Labels: tag.Tags{t1}, ID: s4},
		}},
		Current: shard.Assignment{Placements: map[shard.ID]shard.Placement{
			s1: {Shard: s1, Replicas: []shard.Replica{
				{Node: n1, Role: shard.RolePrimary},
				{Node: n2, Role: shard.RoleSecondary},
				{Node: d1, Role: shard.RoleSecondary},
			}},
			s2: {Shard: s2, Replicas: []shard.Replica{
				{Node: n2, Role: shard.RolePrimary},
				{Node: n3, Role: shard.RoleSecondary},
			}},
			s3: {Shard: s3, Replicas: []shard.Replica{{Node: g1, Role: shard.RolePrimary}}},
			s5: {Shard: s5, Replicas: []shard.Replica{{Node: n3, Role: shard.RolePrimary}}},
		}},
		Nodes: []shard.Node{
			{
				ID:       n3,
				Labels:   tag.Tags{{Key: placementtest.Zone, Value: zoneA}},
				Capacity: map[string]float64{placementtest.CPU: 20},
			},
			{
				ID:       n1,
				Labels:   tag.Tags{{Key: placementtest.Zone, Value: zoneA}},
				Capacity: map[string]float64{placementtest.CPU: 10, placementtest.Disk: 100},
			},
			{
				ID:       n2,
				Labels:   tag.Tags{{Key: placementtest.Zone, Value: zoneB}},
				Capacity: map[string]float64{placementtest.CPU: 10, placementtest.Disk: 100},
			},
		},
		Metrics: []placement.Metric{
			{Name: placementtest.CPU},
			{Name: placementtest.Bytes, Capacity: placementtest.Disk},
		},
		Parallelism: 1,
	}
}

// tally counts the moves that the stateful test of Apply applied.
type tally struct {
	// shards counts the moves of each shard.
	shards map[shard.ID]int
	// nodes counts the moves from and to each node.
	nodes map[shard.NodeID]int
}

// TestState checks NewState, the readers of a State and Apply.
func TestState(t *testing.T) {
	t.Parallel()

	t.Run("has no shards and no nodes when it is zero", func(t *testing.T) {
		t.Parallel()
		var st placement.State
		expect.Empty(t, st.Shards(), "the zero State must have no shards")
		expect.Empty(t, st.Nodes(), "the zero State must have no nodes")
	})

	t.Run("NewState", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrInput for a nil input", func(t *testing.T) {
			t.Parallel()
			_, err := placement.NewState(nil)
			assert.ErrorIs(t, err, placement.ErrInput, "NewState must refuse a nil input")
		})

		t.Run("returns the error of Compile for a model that is not valid", func(t *testing.T) {
			t.Parallel()
			in := cell()
			in.Model = replication.Model{}
			_, err := placement.NewState(in)
			expect.That(t, err).ErrorIs(placement.ErrInput, "NewState must refuse a model without a name").
				ErrorIs(replication.ErrInvalidModel, "the error must wrap the error of Compile")
		})

		tests := []struct {
			name   string
			change func(in *placement.Input)
		}{
			{
				name:   "returns ErrInput for a metric without a name",
				change: func(in *placement.Input) { in.Metrics = append(in.Metrics, placement.Metric{}) },
			},
			{
				name: "returns ErrInput for two metrics of one name",
				change: func(in *placement.Input) {
					in.Metrics = append(in.Metrics, placement.Metric{Name: placementtest.CPU})
				},
			},
			{
				name:   "returns ErrInput for a node without an ID",
				change: func(in *placement.Input) { in.Nodes = append(in.Nodes, shard.Node{}) },
			},
			{
				name:   "returns ErrInput for two nodes of one ID",
				change: func(in *placement.Input) { in.Nodes = append(in.Nodes, shard.Node{ID: n1}) },
			},
			{
				name:   "returns ErrInput for a node that is accepting and draining",
				change: func(in *placement.Input) { in.Draining[n1] = true },
			},
			{
				name:   "returns ErrInput for a draining node without an ID",
				change: func(in *placement.Input) { in.Draining[""] = true },
			},
			{
				name:   "returns ErrInput for a negative capacity",
				change: func(in *placement.Input) { in.Nodes[1].Capacity[placementtest.CPU] = -1 },
			},
			{
				name:   "returns ErrInput for a capacity that is NaN",
				change: func(in *placement.Input) { in.Nodes[2].Capacity[placementtest.Disk] = math.NaN() },
			},
			{
				name: "returns ErrInput for a shard of the layout without an ID",
				change: func(in *placement.Input) {
					in.Layout.Shards = append(in.Layout.Shards, partition.Shard{})
				},
			},
			{
				name: "returns ErrInput for two shards of the layout with one ID",
				change: func(in *placement.Input) {
					in.Layout.Shards = append(in.Layout.Shards, partition.Shard{ID: s4})
				},
			},
			{
				name: "returns ErrInput for a placement of a shard that the layout does not list",
				change: func(in *placement.Input) {
					in.Current.Placements[s9] = shard.Placement{Shard: s9}
				},
			},
			{
				name: "returns ErrInput for a replica without a node",
				change: func(in *placement.Input) {
					in.Current.Placements[s4] = shard.Placement{
						Shard:    s4,
						Replicas: []shard.Replica{{Role: shard.RolePrimary}},
					}
				},
			},
			{
				name: "returns ErrInput for a replica in a role that the model does not declare",
				change: func(in *placement.Input) {
					in.Current.Placements[s4] = shard.Placement{
						Shard:    s4,
						Replicas: []shard.Replica{{Node: n1, Role: shard.RoleLeader}},
					}
				},
			},
			{
				name: "returns ErrInput for a replica in a role that is not valid",
				change: func(in *placement.Input) {
					in.Current.Placements[s4] = shard.Placement{
						Shard:    s4,
						Replicas: []shard.Replica{{Node: n1, Role: shard.RoleLearner + 1}},
					}
				},
			},
			{
				name: "returns ErrInput for two replicas of a shard on one node",
				change: func(in *placement.Input) {
					in.Current.Placements[s4] = shard.Placement{Shard: s4, Replicas: []shard.Replica{
						{Node: n1, Role: shard.RolePrimary},
						{Node: n1, Role: shard.RoleSecondary},
					}}
				},
			},
			{
				name: "returns ErrInput for more replicas in a role than its Max",
				change: func(in *placement.Input) {
					in.Current.Placements[s4] = shard.Placement{Shard: s4, Replicas: []shard.Replica{
						{Node: n1, Role: shard.RolePrimary},
						{Node: n2, Role: shard.RolePrimary},
					}}
				},
			},
			{
				name:   "returns ErrInput for a negative load",
				change: func(in *placement.Input) { in.ShardLoad[s4] = shard.Load{placementtest.CPU: -1} },
			},
			{
				name:   "returns ErrInput for a load that is NaN",
				change: func(in *placement.Input) { in.ShardLoad[s4] = shard.Load{placementtest.Bytes: math.NaN()} },
			},
			{
				name:   "returns ErrInput for an infinite load",
				change: func(in *placement.Input) { in.ShardLoad[s4] = shard.Load{placementtest.CPU: math.Inf(1)} },
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				in := cell()
				tt.change(in)
				_, err := placement.NewState(in)
				assert.ErrorIs(t, err, placement.ErrInput, "NewState must refuse the input")
			})
		}

		t.Run("returns the lowest shard that the layout does not list", func(t *testing.T) {
			t.Parallel()
			in := cell()
			in.Current.Placements[s9] = shard.Placement{Shard: s9}
			in.Current.Placements[s8] = shard.Placement{Shard: s8}
			for b := byte(10); b < 14; b++ {
				extra := shard.ID{ID: id.New128([16]byte{15: b})}
				in.Current.Placements[extra] = shard.Placement{Shard: extra}
			}
			assert.Deterministic(t, func(in *placement.Input) (string, error) {
				_, err := placement.NewState(in)
				return fmt.Sprint(err), nil
			}, in, "NewState must return the same error on every call")
			_, err := placement.NewState(in)
			assert.ErrorIs(t, err, placement.ErrInput, "NewState must refuse a placement outside the layout")
			expect.Contains(t, err.Error(), s8.String(), "the error must name the lowest shard outside the layout")
			expect.NotContains(t, err.Error(), s9.String(), "the error must not name a higher shard")
		})

		t.Run("returns the lowest node that is accepting and draining", func(t *testing.T) {
			t.Parallel()
			in := cell()
			in.Draining[n1] = true
			in.Draining[n2] = true
			assert.Deterministic(t, func(in *placement.Input) (string, error) {
				_, err := placement.NewState(in)
				return fmt.Sprint(err), nil
			}, in, "NewState must return the same error on every call")
			_, err := placement.NewState(in)
			assert.ErrorIs(t, err, placement.ErrInput, "NewState must refuse a node that is accepting and draining")
			expect.Contains(t, err.Error(), string(n1), "the error must name the lowest such node")
			expect.NotContains(t, err.Error(), string(n2), "the error must not name a higher node")
		})

		t.Run("returns nil for every generated input", func(t *testing.T) {
			t.Parallel()
			prop.NoError(t, func(in *placement.Input) error {
				_, err := placement.NewState(in)
				return err
			}, "NewState must accept every input of the generator", prop.Using(placementtest.Inputs()))
		})
	})

	st := newState(t, cell())

	t.Run("Shards", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the shards of the layout in the order of their IDs", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, st.Shards(), []shard.ID{s1, s2, s3, s4, s5}, "Shards must list every shard in ID order")
		})
	})

	t.Run("Shard", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the shard of the layout with its labels", func(t *testing.T) {
			t.Parallel()
			got, ok := st.Shard(s2)
			assert.True(t, ok, "Shard must find a shard of the layout")
			assert.Equal(t, got, partition.Shard{Labels: tag.Tags{t0, homeA}, ID: s2},
				"Shard must return the shard of the layout")
		})

		t.Run("reports false for a shard that the layout does not list", func(t *testing.T) {
			t.Parallel()
			_, ok := st.Shard(s9)
			assert.False(t, ok, "Shard must report false for a shard outside the layout")
		})
	})

	t.Run("Replicas", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the replicas of the current placement", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, st.Replicas(s2), []shard.Replica{
				{Node: n2, Role: shard.RolePrimary},
				{Node: n3, Role: shard.RoleSecondary},
			}, "Replicas must return the replicas of Input.Current")
		})

		t.Run("returns nil for a shard without a placement", func(t *testing.T) {
			t.Parallel()
			assert.Nil(t, st.Replicas(s4), "Replicas must return nil for a shard without a placement")
		})

		t.Run("returns nil for a shard that the layout does not list", func(t *testing.T) {
			t.Parallel()
			assert.Nil(t, st.Replicas(s9), "Replicas must return nil for a shard outside the layout")
		})
	})

	t.Run("ReplicasAt", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the replicas of the shard at a position of Shards", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, st.ReplicasAt(1), st.Replicas(s2), "ReplicasAt must return the replicas of s2")
		})

		t.Run("panics for a position that Shards does not have", func(t *testing.T) {
			t.Parallel()
			assert.Panics(t, func() { _ = st.ReplicasAt(len(st.Shards())) },
				"ReplicasAt must panic for a position after the last shard")
		})
	})

	t.Run("InitialAt", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the initial replicas of the shard at a position of Shards", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, st.InitialAt(2), st.Initial(s3), "InitialAt must return the initial replicas of s3")
		})

		t.Run("panics for a position that Shards does not have", func(t *testing.T) {
			t.Parallel()
			assert.Panics(t, func() { _ = st.InitialAt(-1) }, "InitialAt must panic for a negative position")
		})
	})

	t.Run("Initial", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the replicas of the current placement", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, st.Initial(s3), []shard.Replica{{Node: g1, Role: shard.RolePrimary}},
				"Initial must return the replicas of Input.Current")
		})

		t.Run("returns nil for a shard that the layout does not list", func(t *testing.T) {
			t.Parallel()
			assert.Nil(t, st.Initial(s9), "Initial must return nil for a shard outside the layout")
		})
	})

	t.Run("InFlight", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the transition in flight of a shard", func(t *testing.T) {
			t.Parallel()
			got, ok := st.InFlight(s5)
			assert.True(t, ok, "InFlight must report the transition of s5")
			assert.Equal(t, got, placement.InFlight{To: n1, Fallback: n3}, "InFlight must return the transition")
		})

		t.Run("reports false for a shard without a transition", func(t *testing.T) {
			t.Parallel()
			_, ok := st.InFlight(s1)
			assert.False(t, ok, "InFlight must report false for a shard without a transition")
		})

		t.Run("reports false for a shard that the layout does not list", func(t *testing.T) {
			t.Parallel()
			in := cell()
			in.Transitions[s9] = placement.InFlight{To: n1}
			in.Transitions[s1] = placement.InFlight{To: n3}
			_, ok := newState(t, in).InFlight(s9)
			assert.False(t, ok, "InFlight must report false for a shard outside the layout")
		})
	})

	t.Run("ShardLoad", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the load that the input reports", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, st.ShardLoad(s3), shard.Load{placementtest.CPU: 1},
				"ShardLoad must return the entry of Input.ShardLoad")
		})

		t.Run("returns the mean load for a shard without a report", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, st.ShardLoad(s4), shard.Load{placementtest.CPU: 2.5, placementtest.Bytes: 11.25},
				"ShardLoad must return the mean of the four reported loads")
		})

		t.Run("returns a load of zero for a cell without reports", func(t *testing.T) {
			t.Parallel()
			in := cell()
			in.ShardLoad = nil
			assert.Equal(t, newState(t, in).ShardLoad(s1), shard.Load{placementtest.CPU: 0, placementtest.Bytes: 0},
				"ShardLoad must return a load of zero on each metric")
		})

		t.Run("returns nil for a shard that the layout does not list", func(t *testing.T) {
			t.Parallel()
			assert.Nil(t, st.ShardLoad(s9), "ShardLoad must return nil for a shard outside the layout")
		})
	})

	t.Run("Desired", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the sum of the Max of the roles", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, st.Desired(), 3, "Desired must return one primary and two secondaries")
		})
	})

	t.Run("Max", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			give shard.Role
			want int
		}{
			{name: "returns the Max of a role of the model", give: shard.RoleSecondary, want: 2},
			{name: "returns 0 for a role that the model does not declare", give: shard.RoleLeader},
			{name: "returns 0 for a role that is not valid", give: shard.RoleLearner + 1},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, st.Max(tt.give), tt.want, "Max must return the Max of the role")
			})
		}
	})

	t.Run("Nodes", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the accepting nodes in the order of their IDs", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, st.Nodes(), []shard.NodeID{n1, n2, n3}, "Nodes must list the accepting nodes in order")
		})
	})

	t.Run("Node", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the description of an accepting node", func(t *testing.T) {
			t.Parallel()
			got, ok := st.Node(n3)
			assert.True(t, ok, "Node must find an accepting node")
			assert.Equal(t, got, cell().Nodes[0], "Node must return the node of the input")
		})

		tests := []struct {
			name string
			give shard.NodeID
		}{
			{name: "reports false for a draining node", give: d1},
			{name: "reports false for a gone node", give: g1},
			{name: "reports false for a node that the cell does not have", give: unknown},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				_, ok := st.Node(tt.give)
				assert.False(t, ok, "Node must report false for a node that does not accept replicas")
			})
		}
	})

	t.Run("Draining", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			give shard.NodeID
			want bool
		}{
			{name: "reports true for a draining node", give: d1, want: true},
			{name: "reports false for a node that the input lists as false", give: x1},
			{name: "reports false for an accepting node", give: n1},
			{name: "reports false for a node that the cell does not have", give: unknown},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, st.Draining(tt.give), tt.want, "Draining must report whether the node drains")
			})
		}

		t.Run("reports false for an unknown node of a cell whose first node drains", func(t *testing.T) {
			t.Parallel()
			in := cell()
			in.Nodes = nil
			assert.False(t, newState(t, in).Draining(unknown), "Draining must report false for an unknown node")
		})
	})

	t.Run("OnNode", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			give shard.NodeID
			want []shard.ID
		}{
			{name: "returns the shards with a replica on an accepting node", give: n3, want: []shard.ID{s2, s5}},
			{name: "returns the shards with a replica on a draining node", give: d1, want: []shard.ID{s1}},
			{name: "returns the shards with a replica on a gone node", give: g1, want: []shard.ID{s3}},
			{name: "returns no shard for the destination of a transition", give: n1, want: []shard.ID{s1}},
			{name: "returns nil for a node that the cell does not have", give: unknown},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, st.OnNode(tt.give), tt.want, "OnNode must list the shards of the node in ID order")
			})
		}
	})

	t.Run("RoleCount", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			node shard.NodeID
			role shard.Role
			want int
		}{
			{name: "returns the primaries of a node", node: n2, role: shard.RolePrimary, want: 1},
			{name: "returns the secondaries of a node", node: n3, role: shard.RoleSecondary, want: 1},
			{name: "returns the replicas of a draining node", node: d1, role: shard.RoleSecondary, want: 1},
			{name: "returns 0 for a role without replicas on the node", node: n1, role: shard.RoleSecondary},
			{name: "returns 0 for a role that is not valid", node: n1, role: shard.RoleLearner + 1},
			{name: "returns 0 for a node that the cell does not have", node: unknown, role: shard.RolePrimary},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(
					t,
					st.RoleCount(tt.node, tt.role),
					tt.want,
					"RoleCount must count the replicas of the role",
				)
			})
		}
	})

	t.Run("RoleTotal", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			give shard.Role
			want int
		}{
			{name: "returns the primaries on the accepting nodes", give: shard.RolePrimary, want: 3},
			{name: "returns the secondaries on the accepting nodes", give: shard.RoleSecondary, want: 2},
			{name: "returns 0 for a role that is not valid", give: shard.RoleLearner + 1},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, st.RoleTotal(tt.give), tt.want, "RoleTotal must count the replicas on accepting nodes")
			})
		}
	})

	t.Run("NodeLoad", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			give shard.NodeID
			want shard.Load
		}{
			{
				name: "returns the load of the replicas and the destinations of a node",
				give: n1, want: shard.Load{placementtest.CPU: 5, placementtest.Bytes: 15},
			},
			{
				name: "returns the load of a fallback once with the replica on the node",
				give: n3, want: shard.Load{placementtest.CPU: 7, placementtest.Bytes: 35},
			},
			{
				name: "returns the load of a gone node",
				give: g1, want: shard.Load{placementtest.CPU: 1, placementtest.Bytes: 0},
			},
			{
				name: "returns nil for a node that the state does not know",
				give: x1, want: nil,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, st.NodeLoad(tt.give), tt.want, "NodeLoad must sum the loads that count on the node")
			})
		}

		t.Run("returns nil for the empty fallback of a transition", func(t *testing.T) {
			t.Parallel()
			in := cell()
			in.Transitions[s5] = placement.InFlight{To: n1}
			assert.Nil(t, newState(t, in).NodeLoad(""), "NodeLoad must not describe the empty node")
		})
	})

	t.Run("PeakLoad", func(t *testing.T) {
		t.Parallel()

		t.Run("returns NodeLoad before a move", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, st.PeakLoad(n2), st.NodeLoad(n2), "PeakLoad must equal NodeLoad before a move")
		})

		t.Run("returns nil for a node that the cell does not have", func(t *testing.T) {
			t.Parallel()
			assert.Nil(t, st.PeakLoad(unknown), "PeakLoad must return nil for an unknown node")
		})
	})

	t.Run("PeakCounts", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name  string
			node  shard.NodeID
			shard shard.ID
			want  bool
		}{
			{name: "reports true for a replica of the shard on the node", node: n2, shard: s1, want: true},
			{name: "reports true for the destination of a transition", node: n1, shard: s5, want: true},
			{name: "reports true for the fallback of a transition", node: n3, shard: s5, want: true},
			{name: "reports false for a node without the shard", node: n1, shard: s2},
			{name: "reports false for a shard without a transition", node: n3, shard: s1},
			{name: "reports false for the empty node", node: "", shard: s5},
			{name: "reports false for a shard that the layout does not list", node: n1, shard: s9},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, st.PeakCounts(tt.node, tt.shard), tt.want,
					"PeakCounts must report whether the peak load of the node counts the shard")
			})
		}

		t.Run("reports false for an empty fallback of a transition", func(t *testing.T) {
			t.Parallel()
			in := cell()
			in.Transitions[s5] = placement.InFlight{To: n1}
			assert.False(t, newState(t, in).PeakCounts("", s5), "PeakCounts must not count the empty node")
		})
	})

	t.Run("Metrics", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the metrics with the names of their capacities", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, st.Metrics(), []placement.Metric{
				{Name: placementtest.CPU, Capacity: placementtest.CPU},
				{Name: placementtest.Bytes, Capacity: placementtest.Disk},
			}, "Metrics must name the capacity of each metric")
		})
	})

	t.Run("Capacity", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name   string
			node   shard.NodeID
			metric string
			want   float64
			wantOK bool
		}{
			{
				name:   "returns the capacity that a metric fills",
				node:   n2,
				metric: placementtest.Bytes,
				want:   100,
				wantOK: true,
			},
			{
				name:   "reports false for a capacity that the node does not declare",
				node:   n3,
				metric: placementtest.Bytes,
			},
			{name: "reports false for a metric that the cell does not have", node: n1, metric: mem},
			{name: "reports false for a draining node", node: d1, metric: placementtest.CPU},
			{name: "reports false for a node that the cell does not have", node: unknown, metric: placementtest.CPU},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				got, ok := st.Capacity(tt.node, tt.metric)
				expect.Equal(t, ok, tt.wantOK, "Capacity must report whether the node declares the capacity")
				expect.Equal(t, got, tt.want, "Capacity must return the capacity of the node")
			})
		}
	})

	t.Run("Utilisation", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name   string
			node   shard.NodeID
			metric string
			want   float64
			wantOK bool
		}{
			{
				name:   "returns the load divided by the capacity",
				node:   n3,
				metric: placementtest.CPU,
				want:   0.35,
				wantOK: true,
			},
			{
				name:   "reports false for a capacity that the node does not declare",
				node:   n3,
				metric: placementtest.Bytes,
			},
			{name: "reports false for a draining node", node: d1, metric: placementtest.CPU},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				got, ok := st.Utilisation(tt.node, tt.metric)
				expect.Equal(t, ok, tt.wantOK, "Utilisation must report whether the node has a positive capacity")
				expect.Equal(t, got, tt.want, "Utilisation must divide the load by the capacity")
			})
		}

		t.Run("reports false for a capacity of zero", func(t *testing.T) {
			t.Parallel()
			in := cell()
			in.Nodes[1].Capacity[placementtest.CPU] = 0
			_, ok := newState(t, in).Utilisation(n1, placementtest.CPU)
			assert.False(t, ok, "Utilisation must report false for a node with a capacity of zero")
		})
	})

	t.Run("UtilisationSum", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the sum over the accepting nodes with a capacity", func(t *testing.T) {
			t.Parallel()
			sum, count := st.UtilisationSum(placementtest.Bytes)
			expect.Equal(t, count, 2, "UtilisationSum must count n1 and n2")
			expect.CloseTo(t, sum, 0.55, 1e-12, "UtilisationSum must add 0.15 and 0.4")
		})

		t.Run("returns zero for a metric that the cell does not have", func(t *testing.T) {
			t.Parallel()
			sum, count := st.UtilisationSum(mem)
			expect.Equal(t, count, 0, "UtilisationSum must count no node")
			expect.Equal(t, sum, 0.0, "UtilisationSum must return a sum of zero")
		})
	})

	t.Run("TotalLoad", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name   string
			metric string
			want   float64
		}{
			{
				name:   "returns the reported loads and the mean load of a shard without a report",
				metric: placementtest.CPU,
				want:   12.5,
			},
			{
				name:   "counts a load of 0 for a metric that a report leaves out",
				metric: placementtest.Bytes,
				want:   56.25,
			},
			{name: "returns 0 for a metric that the cell does not have", metric: mem},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, st.TotalLoad(tt.metric), tt.want, "TotalLoad must add the load of every shard once")
			})
		}
	})

	t.Run("Labelled", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name  string
			label tag.Tag
			node  shard.NodeID
			want  int
		}{
			{name: "returns the replicas of the shards with the label on a node", label: t0, node: n2, want: 2},
			{name: "returns the replicas on a draining node", label: t0, node: d1, want: 1},
			{name: "returns 0 for a label without replicas", label: t1, node: n1},
			{name: "returns 0 for a node that the cell does not have", label: t0, node: unknown},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, st.Labelled(tt.label, tt.node), tt.want, "Labelled must count the replicas on the node")
			})
		}

		t.Run("counts the first tag of a key alone", func(t *testing.T) {
			t.Parallel()
			in := cell()
			in.Layout.Shards[2].Labels = tag.Tags{t1, t0}
			got := newState(t, in)
			expect.Equal(t, got.Labelled(t1, n3), 1, "Labelled must count the first tag of the key")
			expect.Equal(t, got.Labelled(t0, n3), 1, "Labelled must not count a later tag of the same key")
		})
	})

	t.Run("LabelledNodes", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the nodes with a replica of a shard with the label", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, st.LabelledNodes(t0), 4, "LabelledNodes must count n1, n2, n3 and d1")
		})

		t.Run("returns 0 for a label without replicas", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, st.LabelledNodes(t1), 0, "LabelledNodes must count no node for an unplaced label")
		})
	})

	t.Run("Moved", func(t *testing.T) {
		t.Parallel()

		t.Run("returns 0 before a move", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, st.Moved(), 0, "Moved must count no move of a new state")
		})
	})

	// shifted is the state of the cell after a move of the primary of s1, the first
	// shard, off n1, the first node.
	shifted := newState(t, cell())
	assert.NoError(t, shifted.Apply(placement.Move{From: n1, To: n3, Shard: s1, Role: shard.RolePrimary}),
		"Apply must move the primary of s1 to n3")

	t.Run("MovedNode", func(t *testing.T) {
		t.Parallel()

		t.Run("returns 0 for a node that the cell does not have", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, shifted.MovedNode(unknown), 0, "MovedNode must count no move of an unknown node")
		})
	})

	t.Run("MovedShard", func(t *testing.T) {
		t.Parallel()

		t.Run("returns 0 for a shard that the layout does not list", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, shifted.MovedShard(s9), 0, "MovedShard must count no move of an unknown shard")
		})
	})

	t.Run("Apply", func(t *testing.T) {
		t.Parallel()

		refused := []struct {
			name string
			give placement.Move
		}{
			{
				name: "returns ErrMove for a shard that the layout does not list",
				give: placement.Move{To: n1, Shard: s9, Role: shard.RolePrimary},
			},
			{
				name: "returns ErrMove for a shard with a transition in flight",
				give: placement.Move{From: n3, To: n2, Shard: s5, Role: shard.RolePrimary},
			},
			{
				name: "returns ErrMove for a move without a From and a To",
				give: placement.Move{Shard: s4, Role: shard.RolePrimary},
			},
			{
				name: "returns ErrMove for a move whose From equals its To",
				give: placement.Move{From: n1, To: n1, Shard: s1, Role: shard.RolePrimary},
			},
			{
				name: "returns ErrMove for a role that the model does not declare",
				give: placement.Move{To: n1, Shard: s4, Role: shard.RoleLeader},
			},
			{
				name: "returns ErrMove for a role that is not valid",
				give: placement.Move{To: n1, Shard: s4, Role: shard.RoleLearner + 1},
			},
			{
				name: "returns ErrMove for a From without a replica in the role",
				give: placement.Move{From: n1, Shard: s1, Role: shard.RoleSecondary},
			},
			{
				name: "returns ErrMove for a draining To",
				give: placement.Move{To: d1, Shard: s4, Role: shard.RolePrimary},
			},
			{
				name: "returns ErrMove for a gone To",
				give: placement.Move{From: n1, To: g1, Shard: s1, Role: shard.RolePrimary},
			},
			{
				name: "returns ErrMove for a To that the cell does not have",
				give: placement.Move{To: unknown, Shard: s4, Role: shard.RolePrimary},
			},
			{
				name: "returns ErrMove for a To with a replica of the shard",
				give: placement.Move{From: d1, To: n2, Shard: s1, Role: shard.RoleSecondary},
			},
			{
				name: "returns ErrMove for a role in which the shard has Max replicas",
				give: placement.Move{To: n3, Shard: s1, Role: shard.RolePrimary},
			},
		}
		for _, tt := range refused {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				got := newState(t, cell())
				assert.ErrorIs(t, got.Apply(tt.give), placement.ErrMove, "Apply must refuse the move")
				expect.Equal(t, got.Moved(), 0, "a refused move must not count")
				expect.Equal(t, got.Replicas(tt.give.Shard), cell().Current.Placements[tt.give.Shard].Replicas,
					"a refused move must not change the replicas")
			})
		}

		t.Run("returns ErrMove for every move of the zero State", func(t *testing.T) {
			t.Parallel()
			var zero placement.State
			assert.ErrorIs(t, zero.Apply(placement.Move{To: n1, Shard: s1, Role: shard.RolePrimary}), placement.ErrMove,
				"Apply must refuse a move of the zero State")
		})

		t.Run("relocates a replica in its place", func(t *testing.T) {
			t.Parallel()
			got := newState(t, cell())
			assert.NoError(t, got.Apply(placement.Move{From: d1, To: n3, Shard: s1, Role: shard.RoleSecondary}),
				"Apply must relocate the secondary of s1")
			expect.Equal(t, got.Replicas(s1), []shard.Replica{
				{Node: n1, Role: shard.RolePrimary},
				{Node: n2, Role: shard.RoleSecondary},
				{Node: n3, Role: shard.RoleSecondary},
			}, "Apply must keep the position of the replica")
			expect.Equal(t, got.Initial(s1), cell().Current.Placements[s1].Replicas,
				"Apply must not change the initial replicas")
			expect.Equal(t, got.OnNode(n3), []shard.ID{s1, s2, s5}, "Apply must insert s1 into the list of n3")
			expect.Empty(t, got.OnNode(d1), "Apply must remove s1 from the list of d1")
			expect.Equal(t, got.RoleTotal(shard.RoleSecondary), 3, "a secondary must move to an accepting node")
			expect.Equal(t, got.NodeLoad(d1), shard.Load{placementtest.CPU: 0, placementtest.Bytes: 0},
				"the load of s1 must leave d1")
			expect.Equal(t, got.PeakLoad(d1), shard.Load{placementtest.CPU: 2, placementtest.Bytes: 10},
				"the peak load of d1 must keep s1, which it hosted at the start")
			expect.Equal(t, got.NodeLoad(n3)[placementtest.CPU], 9.0, "the load of s1 must count on n3")
			expect.Equal(t, got.PeakLoad(n3)[placementtest.CPU], 9.0, "the peak load of n3 must count s1")
			expect.Equal(t, got.Labelled(t0, n3), 2, "the label of s1 must count on n3")
			expect.Equal(t, got.LabelledNodes(t0), 3, "d1 must no longer count for the label of s1")
			expect.Equal(t, got.Moved(), 1, "Moved must count the move")
			expect.Equal(t, got.MovedShard(s1), 1, "MovedShard must count the move of s1")
			expect.Equal(t, got.MovedNode(d1), 1, "MovedNode must count the move from d1")
			expect.Equal(t, got.MovedNode(n3), 1, "MovedNode must count the move to n3")
		})

		t.Run("adds a replica", func(t *testing.T) {
			t.Parallel()
			got := newState(t, cell())
			assert.NoError(t, got.Apply(placement.Move{To: n1, Shard: s4, Role: shard.RolePrimary}),
				"Apply must add the primary of s4")
			expect.Equal(t, got.Replicas(s4), []shard.Replica{{Node: n1, Role: shard.RolePrimary}},
				"Apply must append the replica")
			expect.Equal(t, got.NodeLoad(n1)[placementtest.CPU], 7.5, "the mean load of s4 must count on n1")
			expect.Equal(t, got.PeakLoad(n1)[placementtest.CPU], 7.5, "the peak load of n1 must count s4")
			sum, _ := got.UtilisationSum(placementtest.CPU)
			expect.CloseTo(t, sum, 1.7, 1e-12, "the utilisation of n1 must rise by 0.25")
		})

		t.Run("removes a replica", func(t *testing.T) {
			t.Parallel()
			got := newState(t, cell())
			assert.NoError(t, got.Apply(placement.Move{From: n3, Shard: s2, Role: shard.RoleSecondary}),
				"Apply must remove the secondary of s2")
			expect.Equal(t, got.Replicas(s2), []shard.Replica{{Node: n2, Role: shard.RolePrimary}},
				"Apply must delete the replica")
			expect.Equal(t, got.NodeLoad(n3)[placementtest.CPU], 3.0, "the load of s2 must leave n3")
			expect.Equal(t, got.PeakLoad(n3)[placementtest.CPU], 7.0, "the peak load of n3 must keep s2")
		})

		t.Run("returns the peak load of a node that a shard leaves again", func(t *testing.T) {
			t.Parallel()
			got := newState(t, cell())
			assert.NoError(t, got.Apply(placement.Move{From: n3, To: n1, Shard: s2, Role: shard.RoleSecondary}),
				"Apply must relocate the secondary of s2 to n1")
			assert.NoError(t, got.Apply(placement.Move{From: n1, To: n3, Shard: s2, Role: shard.RoleSecondary}),
				"Apply must relocate the secondary of s2 back to n3")
			expect.Equal(t, got.PeakLoad(n1), newState(t, cell()).PeakLoad(n1),
				"the peak load of n1 must lose s2, which n1 did not host at the start")
			expect.Equal(t, got.PeakLoad(n3), newState(t, cell()).PeakLoad(n3),
				"the peak load of n3 must count s2 once")
		})

		t.Run("keeps every index of the state equal to its definition", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "the indexes of a state follow its replicas after every move", func(c *prop.Case) {
				in := c.Draw(placementtest.Inputs(), labelInput)
				state := newState(c, in)
				replicas := make(map[shard.ID][]shard.Replica)
				for _, id := range state.Shards() {
					replicas[id] = slices.Clone(state.Replicas(id))
				}
				moves := tally{shards: map[shard.ID]int{}, nodes: map[shard.NodeID]int{}}
				stateful.Steps(c, stateful.Machine[struct{}]{
					Actions: []stateful.Action[struct{}]{{
						Name:    "apply",
						Enabled: func(struct{}) bool { return len(placementtest.Moves(state)) > 0 },
						Input: func(c *prop.Case, _ struct{}) any {
							all := placementtest.Moves(state)
							return all[c.Draw(prop.Integer(0, len(all)-1), labelMove)]
						},
						Run: func(c *prop.Case, _ int, v any) {
							m := v.(placement.Move)
							assert.NoError(c, state.Apply(m), "Apply must accept a move that Moves lists")
							replicas[m.Shard] = moved(replicas[m.Shard], m)
							moves.shards[m.Shard]++
							for _, n := range [...]shard.NodeID{m.From, m.To} {
								if n != "" {
									moves.nodes[n]++
								}
							}
						},
					}},
					Invariant: func(c *prop.Case, _ struct{}) { assertIndexes(c, in, state, replicas, moves) },
				})
			})
		})
	})
}

// TestStateAllocs checks the allocation contract of NewState, of the readers of a State
// and of Apply. MaxAllocs counts the allocations of the whole process, so the test
// does not run in parallel.
func TestStateAllocs(t *testing.T) {
	t.Run("NewState", func(t *testing.T) {
		// The fifth node of the cell is gone, so NewState adds it while it reads the
		// replicas.
		in := scaled(1_000, 5)
		in.Nodes = in.Nodes[:4]
		expect.MaxAllocs(t, func() { _, _ = placement.NewState(in) }, 64,
			"NewState must allocate 64 objects for a cell of 1,000 shards on 4 accepting nodes and a gone node")
	})

	st := newState(t, cell())
	readers := []struct {
		name string
		call func()
	}{
		{name: "Shards", call: func() { _ = st.Shards() }},
		{name: "Shard", call: func() { _, _ = st.Shard(s2) }},
		{name: "Replicas", call: func() { _ = st.Replicas(s1) }},
		{name: "ReplicasAt", call: func() { _ = st.ReplicasAt(0) }},
		{name: "Initial", call: func() { _ = st.Initial(s1) }},
		{name: "InitialAt", call: func() { _ = st.InitialAt(0) }},
		{name: "InFlight", call: func() { _, _ = st.InFlight(s5) }},
		{name: "ShardLoad", call: func() { _ = st.ShardLoad(s4) }},
		{name: "Desired", call: func() { _ = st.Desired() }},
		{name: "Max", call: func() { _ = st.Max(shard.RoleSecondary) }},
		{name: "Nodes", call: func() { _ = st.Nodes() }},
		{name: "Node", call: func() { _, _ = st.Node(n1) }},
		{name: "Draining", call: func() { _ = st.Draining(d1) }},
		{name: "OnNode", call: func() { _ = st.OnNode(n3) }},
		{name: "RoleCount", call: func() { _ = st.RoleCount(n2, shard.RolePrimary) }},
		{name: "RoleTotal", call: func() { _ = st.RoleTotal(shard.RolePrimary) }},
		{name: "NodeLoad", call: func() { _ = st.NodeLoad(n1) }},
		{name: "PeakLoad", call: func() { _ = st.PeakLoad(n1) }},
		{name: "PeakCounts", call: func() { _ = st.PeakCounts(n1, s5) }},
		{name: "Metrics", call: func() { _ = st.Metrics() }},
		{name: "Capacity", call: func() { _, _ = st.Capacity(n1, placementtest.Bytes) }},
		{name: "Utilisation", call: func() { _, _ = st.Utilisation(n1, placementtest.Bytes) }},
		{name: "UtilisationSum", call: func() { _, _ = st.UtilisationSum(placementtest.Bytes) }},
		{name: "TotalLoad", call: func() { _ = st.TotalLoad(placementtest.Bytes) }},
		{name: "Labelled", call: func() { _ = st.Labelled(t0, n2) }},
		{name: "LabelledNodes", call: func() { _ = st.LabelledNodes(t0) }},
		{name: "Moved", call: func() { _ = st.Moved() }},
		{name: "MovedNode", call: func() { _ = st.MovedNode(n1) }},
		{name: "MovedShard", call: func() { _ = st.MovedShard(s1) }},
	}
	for _, tt := range readers {
		t.Run(tt.name, func(t *testing.T) {
			expect.MaxAllocs(t, tt.call, 0, tt.name+" must not allocate")
		})
	}

	t.Run("Apply", func(t *testing.T) {
		moving := newState(t, cell())
		there := placement.Move{From: n1, To: n2, Shard: s3, Role: shard.RolePrimary}
		back := placement.Move{From: n2, To: n1, Shard: s3, Role: shard.RolePrimary}
		for _, m := range []placement.Move{{From: g1, To: n1, Shard: s3, Role: shard.RolePrimary}, there, back} {
			assert.NoError(t, moving.Apply(m), "Apply must accept the moves that grow the lists of n1 and n2")
		}
		var err error
		expect.MaxAllocs(t, func() {
			if err = moving.Apply(there); err == nil {
				err = moving.Apply(back)
			}
		}, 0, "Apply must not allocate for a shard and lists that earlier moves changed")
		assert.NoError(t, err, "the test must measure two moves that Apply accepts")
	})
}

// BenchmarkState reports the cost of NewState for cells of increasing size, and of the
// readers that constraints and goals call for each evaluation.
func BenchmarkState(b *testing.B) {
	b.Run("NewState", func(b *testing.B) {
		for _, shards := range []int{1_000, 10_000, 100_000} {
			in := scaled(shards, shards/200)
			b.Run(strconv.Itoa(shards), func(b *testing.B) {
				var err error

				c := bench.Start(b)
				defer c.End()

				for c.Loop() {
					_, err = placement.NewState(in)
				}

				assert.NoError(b, err, "the benchmark must measure a state that NewState builds")
			})
		}
	})

	st := newState(b, scaled(10_000, 50))
	sh := st.Shards()[len(st.Shards())/2]
	node := st.Nodes()[len(st.Nodes())/2]

	b.Run("Replicas", func(b *testing.B) {
		var got []shard.Replica

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			got = st.Replicas(sh)
		}

		assert.NotEmpty(b, got, "the benchmark must measure a placed shard")
	})

	b.Run("Utilisation", func(b *testing.B) {
		var ok bool

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			_, ok = st.Utilisation(node, placementtest.CPU)
		}

		assert.True(b, ok, "the benchmark must measure a node with a capacity")
	})

	b.Run("PeakCounts", func(b *testing.B) {
		var got bool

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			got = st.PeakCounts(st.Replicas(sh)[0].Node, sh)
		}

		assert.True(b, got, "the benchmark must measure a node that counts the shard")
	})
}

// newState returns the state of in, and stops tb when NewState refuses in.
func newState(tb assert.TB, in *placement.Input) *placement.State {
	tb.Helper()
	st, err := placement.NewState(in)
	assert.NoError(tb, err, "NewState must accept the input of the test")
	return st
}

// moved returns replicas after m: with the replica of m relocated in its place,
// removed, or appended.
func moved(replicas []shard.Replica, m placement.Move) []shard.Replica {
	i := slices.Index(replicas, shard.Replica{Node: m.From, Role: m.Role})
	if i >= 0 && m.To != "" {
		replicas[i].Node = m.To
		return replicas
	}
	if i >= 0 {
		return slices.Delete(replicas, i, i+1)
	}
	return append(replicas, shard.Replica{Node: m.To, Role: m.Role})
}

// assertIndexes stops tb unless every index of st follows from in, from the replicas
// of each shard in st, and from the moves of each shard and each node. It computes
// each index from its definition and compares it with the reader of st.
func assertIndexes(
	tb assert.TB,
	in *placement.Input,
	st *placement.State,
	replicas map[shard.ID][]shard.Replica,
	moves tally,
) {
	tb.Helper()

	nodes := map[shard.NodeID]bool{}
	labels := map[tag.Tag]bool{}
	for i, id := range st.Shards() {
		assert.Equal(tb, st.Replicas(id), replicas[id], "Replicas must follow the moves", assert.EquateEmpty())
		assert.Equal(tb, st.ReplicasAt(i), st.Replicas(id), "ReplicasAt must return the replicas of the shard at i")
		assert.Equal(tb, st.InitialAt(i), st.Initial(id), "InitialAt must return the initial replicas at i")
		assert.Equal(tb, st.MovedShard(id), moves.shards[id], "MovedShard must count the moves of the shard")
		for _, r := range slices.Concat(st.Initial(id), replicas[id]) {
			nodes[r.Node] = true
		}
		if f, ok := in.Transitions[id]; ok {
			nodes[f.To] = true
			nodes[f.Fallback] = true
		}
		sh, _ := st.Shard(id)
		for _, l := range sh.Labels {
			labels[l] = true
		}
	}
	for _, n := range in.Nodes {
		nodes[n.ID] = true
	}
	delete(nodes, "")

	var total [shard.RoleCount]int
	sums := map[string]float64{}
	labelledNodes := map[tag.Tag]int{}
	for n := range nodes {
		assert.Equal(tb, st.MovedNode(n), moves.nodes[n], "MovedNode must count the moves from and to the node")
		_, accepting := st.Node(n)
		var shards []shard.ID
		var roles [shard.RoleCount]int
		load, peak := map[string]float64{}, map[string]float64{}
		labelled := map[tag.Tag]int{}
		for _, id := range st.Shards() {
			on := func(r shard.Replica) bool { return r.Node == n }
			hosted := slices.IndexFunc(replicas[id], on)
			f, frozen := in.Transitions[id]
			counts := hosted >= 0 || (frozen && (f.To == n || f.Fallback == n))
			peaks := counts || slices.ContainsFunc(st.Initial(id), on)
			assert.Equal(tb, st.PeakCounts(n, id), peaks, "PeakCounts must follow its definition")
			for _, m := range st.Metrics() {
				if counts {
					load[m.Name] += st.ShardLoad(id)[m.Name]
				}
				if peaks {
					peak[m.Name] += st.ShardLoad(id)[m.Name]
				}
			}
			if hosted < 0 {
				continue
			}
			shards = append(shards, id)
			roles[replicas[id][hosted].Role]++
			sh, _ := st.Shard(id)
			for i, l := range sh.Labels {
				if !slices.ContainsFunc(sh.Labels[:i], func(o tag.Tag) bool { return o.Key == l.Key }) {
					labelled[l]++
				}
			}
		}
		assert.Equal(tb, st.OnNode(n), shards, "OnNode must list the shards of the node", assert.EquateEmpty())
		for r := range roles {
			assert.Equal(tb, st.RoleCount(n, shard.Role(r)), roles[r], "RoleCount must count the replicas of the role")
			if accepting {
				total[r] += roles[r]
			}
		}
		for _, m := range st.Metrics() {
			assert.CloseTo(tb, st.NodeLoad(n)[m.Name], load[m.Name], 1e-9, "NodeLoad must follow its definition")
			assert.CloseTo(tb, st.PeakLoad(n)[m.Name], peak[m.Name], 1e-9, "PeakLoad must follow its definition")
			if u, ok := st.Utilisation(n, m.Name); ok {
				sums[m.Name] += u
			}
		}
		for l := range labels {
			assert.Equal(tb, st.Labelled(l, n), labelled[l], "Labelled must count the replicas of the label")
			if labelled[l] > 0 {
				labelledNodes[l]++
			}
		}
	}
	for l := range labels {
		assert.Equal(tb, st.LabelledNodes(l), labelledNodes[l], "LabelledNodes must count the nodes of the label")
	}
	for r := range total {
		assert.Equal(tb, st.RoleTotal(shard.Role(r)), total[r], "RoleTotal must count the accepting replicas")
	}
	for _, m := range st.Metrics() {
		sum, _ := st.UtilisationSum(m.Name)
		assert.CloseTo(tb, sum, sums[m.Name], 1e-9, "UtilisationSum must sum the utilisation")
		total := 0.0
		for _, id := range st.Shards() {
			total += st.ShardLoad(id)[m.Name]
		}
		assert.Equal(tb, st.TotalLoad(m.Name), total, "TotalLoad must add the loads of the shards in ID order")
	}
	applied := 0
	for _, count := range moves.shards {
		applied += count
	}
	assert.Equal(tb, st.Moved(), applied, "Moved must count every move")
}

// scaled returns the input of a cell under primary-only with the given numbers of
// shards and nodes. Shard i has its primary on node i modulo nodes, and a load of cpu
// and bytes.
func scaled(shards, nodes int) *placement.Input {
	in := &placement.Input{
		Model: replication.PrimaryOnly,
		Metrics: []placement.Metric{
			{Name: placementtest.CPU},
			{Name: placementtest.Bytes, Capacity: placementtest.Disk},
		},
		ShardLoad: make(map[shard.ID]shard.Load, shards),
		Current:   shard.Assignment{Placements: make(map[shard.ID]shard.Placement, shards)},
		Layout:    partition.Layout{Cell: cellID, Shards: make([]partition.Shard, shards)},
		Nodes:     make([]shard.Node, nodes),
	}
	for i := range nodes {
		in.Nodes[i] = shard.Node{
			ID:       shard.NodeID("n" + strconv.Itoa(i)),
			Capacity: map[string]float64{placementtest.CPU: 100, placementtest.Disk: 1000},
		}
	}
	for i := range shards {
		sh := shard.ID{ID: id.New128([16]byte{12: byte(i >> 24), 13: byte(i >> 16), 14: byte(i >> 8), 15: byte(i)})}
		in.Layout.Shards[i] = partition.Shard{ID: sh}
		in.Current.Placements[sh] = shard.Placement{
			Shard:    sh,
			Replicas: []shard.Replica{{Node: in.Nodes[i%nodes].ID, Role: shard.RolePrimary}},
		}
		in.ShardLoad[sh] = shard.Load{placementtest.CPU: float64(i % 7), placementtest.Bytes: float64(i % 13)}
	}
	return in
}
