// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placementtest_test

import (
	"errors"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/constraint"
	"go.thesmos.sh/sharder/placement/placementtest"
	"go.thesmos.sh/sharder/shard"
)

// headroom is the headroom of the constraints capacity of the tests.
const headroom = 0.1

// The errors of the constraints that the tests write again.
var (
	errFull     = errors.New("the node is full")
	errMismatch = errors.New("the node does not have the value of the shard")
)

// capacity is the constraint capacity of package constraint, written again from the
// exported methods of placement.State.
type capacity struct{}

// Name returns the name of the built-in constraint capacity.
func (capacity) Name() shard.StrategyName {
	return constraint.CapacityName
}

// Check returns errFull when m puts its destination above a capacity less the
// headroom. It checks the peak load of the destination for a move that places a
// replica, and the load once the moves end for a move whose From equals its To.
func (capacity) Check(st *placement.State, m placement.Move) error {
	if m.To == "" {
		return nil
	}
	node, ok := st.Node(m.To)
	if !ok {
		return nil
	}
	load := st.PeakLoad(m.To)
	if m.From == m.To {
		load = st.NodeLoad(m.To)
	}
	for _, metric := range st.Metrics() {
		limit, ok := node.Capacity[metric.Capacity]
		if !ok {
			continue
		}
		total := load[metric.Name]
		if m.From != m.To && !st.PeakCounts(m.To, m.Shard) {
			total += st.ShardLoad(m.Shard)[metric.Name]
		}
		if total > limit*(1-headroom) {
			return errFull
		}
	}
	return nil
}

// matchLabel is the constraint match-label of package constraint for the label of a
// key, written again from the exported methods of placement.State.
type matchLabel struct {
	// key is the key of the label.
	key string
}

// Name returns the name of the built-in constraint match-label.
func (matchLabel) Name() shard.StrategyName {
	return constraint.MatchLabelName
}

// Check returns errMismatch when the shard of m does not have the label, or when the
// destination of m has another value of it.
func (c matchLabel) Check(st *placement.State, m placement.Move) error {
	if m.To == "" {
		return nil
	}
	sh, _ := st.Shard(m.Shard)
	want, ok := sh.Labels.Find(c.key)
	if !ok {
		return errMismatch
	}
	node, ok := st.Node(m.To)
	if !ok {
		return nil
	}
	if got, ok := node.Labels.Find(c.key); !ok || got.Value != want.Value {
		return errMismatch
	}
	return nil
}

// TestConstraint checks the laws of a constraint against the constraints capacity and
// match-label written again outside package constraint, and checks that they reject
// the moves that the built-ins reject.
func TestConstraint(t *testing.T) {
	t.Parallel()

	builtinCapacity, err := constraint.NewCapacity(constraint.CapacityConfig{Headroom: headroom})
	assert.NoError(t, err, "NewCapacity must accept the headroom")
	builtinMatch, err := constraint.NewMatchLabel(constraint.MatchLabelConfig{Label: placementtest.Tenant})
	assert.NoError(t, err, "NewMatchLabel must accept the label tenant")

	tests := []struct {
		name    string
		give    placement.Constraint
		builtin placement.Constraint
	}{
		{name: "capacity", give: capacity{}, builtin: builtinCapacity},
		{name: "match-label", give: matchLabel{key: placementtest.Tenant}, builtin: builtinMatch},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			t.Run("AssertConstraint", func(t *testing.T) {
				t.Parallel()
				placementtest.AssertConstraint(t, tt.give)
			})

			t.Run("rejects the moves that the built-in rejects", func(t *testing.T) {
				t.Parallel()
				prop.ForAll(t, "the constraint and the built-in reject the same moves", func(c *prop.Case) {
					st := newState(c, c.Draw(placementtest.Inputs(), labelInput))
					moves := placementtest.Moves(st)
					for _, sh := range st.Shards() {
						for _, r := range st.Replicas(sh) {
							moves = append(moves, placement.Move{From: r.Node, To: r.Node, Shard: sh, Role: r.Role})
						}
					}
					for _, m := range moves {
						assert.Equal(c, tt.give.Check(st, m) == nil, tt.builtin.Check(st, m) == nil,
							"the constraint must admit a move exactly when the built-in admits it")
					}
				})
			})
		})
	}
}

// TestConstraintAllocs checks that Check of the constraints written again does not
// allocate.
func TestConstraintAllocs(t *testing.T) {
	t.Run("capacity", func(t *testing.T) {
		placementtest.AssertConstraintAllocs(t, capacity{})
	})

	t.Run("match-label", func(t *testing.T) {
		placementtest.AssertConstraintAllocs(t, matchLabel{key: placementtest.Tenant})
	})
}
