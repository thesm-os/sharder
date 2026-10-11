// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placement_test

import (
	"errors"
	"fmt"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/constraint"
	"go.thesmos.sh/sharder/placement/placementtest"
	"go.thesmos.sh/sharder/shard"
)

// The names of the constraints of the tests of Validate.
const (
	refusingName shard.StrategyName = "refusing"
	limitName    shard.StrategyName = "limit"
	crowdedName  shard.StrategyName = "crowded"
)

// The errors of the constraints of the tests of Validate.
var (
	errRefused = errors.New("refused")
	errFull    = errors.New("full")
	errCrowded = errors.New("crowded")
)

// refusing is a constraint that rejects every replica on one node.
type refusing struct {
	// node is the node that the constraint rejects.
	node shard.NodeID
}

// Name returns the name refusing.
func (refusing) Name() shard.StrategyName {
	return refusingName
}

// Check returns errRefused for a move to the node of r.
func (r refusing) Check(_ *placement.State, m placement.Move) error {
	if m.To == r.node {
		return errRefused
	}
	return nil
}

// limit is a constraint that rejects a replica on a node whose peak load of cpu would
// pass a limit.
type limit struct {
	// cpu is the most peak load of cpu of a node.
	cpu float64
}

// Name returns the name limit.
func (limit) Name() shard.StrategyName {
	return limitName
}

// Check returns errFull when the peak load of cpu of To with the replica of m passes
// the limit of l.
func (l limit) Check(st *placement.State, m placement.Move) error {
	if m.To == "" {
		return nil
	}
	load := st.PeakLoad(m.To)[placementtest.CPU]
	if !st.PeakCounts(m.To, m.Shard) {
		load += st.ShardLoad(m.Shard)[placementtest.CPU]
	}
	if load > l.cpu {
		return errFull
	}
	return nil
}

// crowded is a constraint that rejects a replica that a plan keeps on n1 while n1 has
// more than one shard. It admits every other move.
type crowded struct{}

// Name returns the name crowded.
func (crowded) Name() shard.StrategyName {
	return crowdedName
}

// Check returns errCrowded for a move whose From and To are n1 while n1 has more than
// one shard.
func (crowded) Check(st *placement.State, m placement.Move) error {
	if m.From == n1 && m.To == n1 && len(st.OnNode(n1)) > 1 {
		return errCrowded
	}
	return nil
}

// TestValidate checks the checks of Validate and the violations that it returns.
func TestValidate(t *testing.T) {
	t.Parallel()

	t.Run("Validate", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the errors of NewState", func(t *testing.T) {
			t.Parallel()
			_, err := placement.Validate(nil, placement.Plan{}, nil)
			assert.ErrorIs(t, err, placement.ErrInput, "Validate must refuse an input that NewState refuses")
		})

		tests := []struct {
			name    string
			targets map[shard.ID]placement.Target
			want    []error
			not     []error
		}{
			{
				name:    "returns ErrPlan for a target of a shard that the layout does not list",
				targets: map[shard.ID]placement.Target{s9: {Deficit: 3}},
				want:    []error{placement.ErrPlan},
			},
			{
				name:    "returns ErrPlan for a negative deficit",
				targets: map[shard.ID]placement.Target{s4: {Deficit: -1, Replicas: primaries(n1, n3, n1, n3)}},
				want:    []error{placement.ErrPlan},
				not:     []error{placement.ErrMove},
			},
			{
				name:    "returns ErrPlan for replicas and a deficit that do not add up",
				targets: map[shard.ID]placement.Target{s4: {Deficit: 1, Replicas: primaries(n1)}},
				want:    []error{placement.ErrPlan},
			},
			{
				name: "returns ErrMove for a target that changes a shard with a transition in flight",
				targets: map[shard.ID]placement.Target{s5: {
					Deficit:  2,
					Replicas: []shard.Replica{{Node: n2, Role: shard.RolePrimary}},
				}},
				want: []error{placement.ErrPlan, placement.ErrMove},
			},
			{
				name:    "returns ErrMove for a target that removes a replica of a shard with a transition in flight",
				targets: map[shard.ID]placement.Target{s5: {Deficit: 3}},
				want:    []error{placement.ErrPlan, placement.ErrMove},
			},
			{
				name: "returns ErrMove for a replica on a draining node",
				targets: map[shard.ID]placement.Target{s4: {
					Deficit:  2,
					Replicas: []shard.Replica{{Node: d1, Role: shard.RolePrimary}},
				}},
				want: []error{placement.ErrPlan, placement.ErrMove},
			},
			{
				name: "returns ErrMove for two replicas on one node",
				targets: map[shard.ID]placement.Target{s4: {
					Deficit: 1,
					Replicas: []shard.Replica{
						{Node: n1, Role: shard.RolePrimary},
						{Node: n1, Role: shard.RoleSecondary},
					},
				}},
				want: []error{placement.ErrPlan, placement.ErrMove},
			},
			{
				name: "returns ErrMove for a role that the model does not declare",
				targets: map[shard.ID]placement.Target{s4: {
					Deficit:  2,
					Replicas: []shard.Replica{{Node: n1, Role: shard.RoleLeader}},
				}},
				want: []error{placement.ErrPlan, placement.ErrMove},
			},
			{
				name: "returns ErrMove for more replicas in a role than its Max",
				targets: map[shard.ID]placement.Target{s4: {
					Deficit:  1,
					Replicas: primaries(n1, n2),
				}},
				want: []error{placement.ErrPlan, placement.ErrMove},
			},
			{
				name: "returns ErrPlan for a replica on a node with a fault of the shard",
				targets: map[shard.ID]placement.Target{s2: {
					Deficit:  2,
					Replicas: []shard.Replica{{Node: n3, Role: shard.RolePrimary}},
				}},
				want: []error{placement.ErrPlan},
			},
			{
				name: "returns ErrPlan and the error of a constraint for a replica that it rejects",
				targets: map[shard.ID]placement.Target{s4: {
					Deficit:  2,
					Replicas: []shard.Replica{{Node: n2, Role: shard.RolePrimary}},
				}},
				want: []error{placement.ErrPlan, errRefused},
			},
			{
				name: "returns ErrPlan for two targets that fit the free capacity of a node alone",
				targets: map[shard.ID]placement.Target{
					s4: {Deficit: 2, Replicas: []shard.Replica{{Node: n1, Role: shard.RolePrimary}}},
					s2: {Deficit: 1, Replicas: []shard.Replica{
						{Node: n2, Role: shard.RolePrimary},
						{Node: n1, Role: shard.RoleSecondary},
					}},
				},
				want: []error{placement.ErrPlan, errFull},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				_, err := placement.Validate(cell(), placement.Plan{Targets: tt.targets},
					[]placement.Constraint{refusing{node: n2}, limit{cpu: 10}})
				for _, want := range tt.want {
					expect.ErrorIs(t, err, want, "Validate must reject the plan with each error of the check")
				}
				for _, other := range tt.not {
					expect.ErrorIsNot(
						t,
						err,
						other,
						"Validate must not reject the plan with the error of another check",
					)
				}
			})
		}

		t.Run("returns the error of the lowest shard whose target it rejects", func(t *testing.T) {
			t.Parallel()
			plan := placement.Plan{Targets: map[shard.ID]placement.Target{
				s2: {Deficit: 2, Replicas: []shard.Replica{{Node: n3, Role: shard.RolePrimary}}},
				s3: {Deficit: 1, Replicas: primaries(n1)},
				s4: {Deficit: -1, Replicas: primaries(n1, n2, n3, n1)},
			}}
			assert.Deterministic(t, func(p placement.Plan) (string, error) {
				_, err := placement.Validate(cell(), p, nil)
				return fmt.Sprint(err), nil
			}, plan, "Validate must return the same error on every call")
			_, err := placement.Validate(cell(), plan, nil)
			assert.ErrorIs(t, err, placement.ErrPlan, "Validate must reject the plan")
			expect.Contains(t, err.Error(), s2.String(), "the error must name the lowest shard")
		})

		t.Run("returns nil for each target of two that fits a node alone", func(t *testing.T) {
			t.Parallel()
			cs := []placement.Constraint{limit{cpu: 10}}
			for _, target := range []map[shard.ID]placement.Target{
				{s4: {Deficit: 2, Replicas: []shard.Replica{{Node: n1, Role: shard.RolePrimary}}}},
				{s2: {Deficit: 1, Replicas: []shard.Replica{
					{Node: n2, Role: shard.RolePrimary},
					{Node: n1, Role: shard.RoleSecondary},
				}}},
			} {
				_, err := placement.Validate(cell(), placement.Plan{Targets: target}, cs)
				expect.NoError(t, err, "Validate must accept a target that fits the node alone")
			}
		})

		// movedOff moves the primary of s1 from n1 to n3, and adds the primary of s4 to n1.
		// n1 then has a load of cpu of 5.5, and a peak load of 7.5 while s1 leaves it.
		movedOff := map[shard.ID]placement.Target{
			s1: {Replicas: []shard.Replica{
				{Node: n3, Role: shard.RolePrimary},
				{Node: n2, Role: shard.RoleSecondary},
				{Node: d1, Role: shard.RoleSecondary},
			}},
			s4: {Deficit: 2, Replicas: []shard.Replica{{Node: n1, Role: shard.RolePrimary}}},
		}

		t.Run(
			"returns ErrPlan for a replica that fits a node only after another replica leaves it",
			func(t *testing.T) {
				t.Parallel()
				capacity, err := constraint.NewCapacity(constraint.CapacityConfig{Headroom: 0.3})
				assert.NoError(t, err, "NewCapacity must accept a headroom of 0.3")
				_, err = placement.Validate(cell(), placement.Plan{Targets: movedOff}, []placement.Constraint{capacity})
				expect.That(t, err).ErrorIs(placement.ErrPlan, "Validate must reject the plan").
					ErrorIs(constraint.ErrCapacity, "the error must wrap the error of the constraint")
			},
		)

		t.Run("returns nil for a replica that fits the peak load of its node", func(t *testing.T) {
			t.Parallel()
			capacity, err := constraint.NewCapacity(constraint.CapacityConfig{Headroom: 0.25})
			assert.NoError(t, err, "NewCapacity must accept a headroom of 0.25")
			violations, err := placement.Validate(cell(), placement.Plan{Targets: movedOff},
				[]placement.Constraint{capacity})
			expect.NoError(t, err, "Validate must accept a plan within the capacity of each node")
			expect.Empty(t, violations, "Validate must return no violation for a plan within every capacity")
		})

		t.Run("returns nil for a replica that brings a node to the Max of a constraint", func(t *testing.T) {
			t.Parallel()
			perNode, err := constraint.NewMaxPerNode(constraint.MaxPerNodeConfig{Max: 2})
			assert.NoError(t, err, "NewMaxPerNode must accept a Max of 2")
			violations, err := placement.Validate(cell(), placement.Plan{Targets: map[shard.ID]placement.Target{
				s4: {Deficit: 2, Replicas: []shard.Replica{{Node: n1, Role: shard.RolePrimary}}},
			}}, []placement.Constraint{perNode})
			expect.NoError(t, err, "Validate must count the added replica once")
			expect.Empty(t, violations, "Validate must return no violation for a node at the Max")
		})

		t.Run("returns nil for a target that keeps a shard with a transition in flight", func(t *testing.T) {
			t.Parallel()
			target := placement.Target{Deficit: 2, Replicas: []shard.Replica{{Node: n3, Role: shard.RolePrimary}}}
			violations, err := placement.Validate(cell(), placement.Plan{Targets: map[shard.ID]placement.Target{
				s5: target,
			}}, nil)
			expect.NoError(t, err, "Validate must accept a target that leaves the shard unchanged")
			expect.Empty(t, violations, "Validate must return no violation without constraints")
		})

		t.Run("returns nil for a target that repairs what it can", func(t *testing.T) {
			t.Parallel()
			_, err := placement.Validate(cell(), placement.Plan{Targets: map[shard.ID]placement.Target{
				s3: {Deficit: 2, Replicas: []shard.Replica{{Node: n2, Role: shard.RolePrimary}}},
				s4: {Deficit: 3},
			}}, []placement.Constraint{limit{cpu: 10}})
			assert.NoError(t, err, "Validate must accept targets with a deficit")
		})

		t.Run("returns a violation for each replica that the plan keeps and a constraint rejects", func(t *testing.T) {
			t.Parallel()
			violations, err := placement.Validate(cell(), placement.Plan{Targets: map[shard.ID]placement.Target{
				s1: {Deficit: 1, Replicas: []shard.Replica{
					{Node: n1, Role: shard.RolePrimary},
					{Node: n2, Role: shard.RoleSecondary},
				}},
			}}, []placement.Constraint{limit{cpu: 6}, refusing{node: n2}})
			assert.NoError(t, err, "Validate must not reject a plan for the replicas that it keeps")
			assert.Equal(t, violations, []placement.Violation{
				{Err: errRefused, Constraint: refusingName, Node: n2, Shard: s1, Role: shard.RoleSecondary},
				{Err: errRefused, Constraint: refusingName, Node: n2, Shard: s2, Role: shard.RolePrimary},
				{Err: errFull, Constraint: limitName, Node: n3, Shard: s2, Role: shard.RoleSecondary},
				{Err: errFull, Constraint: limitName, Node: n3, Shard: s5, Role: shard.RolePrimary},
			}, "Validate must return the violations in the order of the shards, the replicas and the constraints")
		})

		t.Run("returns a violation of a kept replica on a node to which the plan adds a replica", func(t *testing.T) {
			t.Parallel()
			violations, err := placement.Validate(cell(), placement.Plan{Targets: map[shard.ID]placement.Target{
				s4: {Deficit: 2, Replicas: []shard.Replica{{Node: n1, Role: shard.RolePrimary}}},
			}}, []placement.Constraint{crowded{}})
			assert.NoError(t, err, "Validate must accept the replica that the plan adds")
			assert.Equal(t, violations, []placement.Violation{
				{Err: errCrowded, Constraint: crowdedName, Node: n1, Shard: s1, Role: shard.RolePrimary},
			}, "Validate must check the kept replicas alone against the state of the whole plan")
		})
	})
}

// primaries returns a replica in the role primary on each node of nodes.
func primaries(nodes ...shard.NodeID) []shard.Replica {
	replicas := make([]shard.Replica, len(nodes))
	for i, n := range nodes {
		replicas[i] = shard.Replica{Node: n, Role: shard.RolePrimary}
	}
	return replicas
}
