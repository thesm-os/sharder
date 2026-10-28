// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package constraint_test

import (
	"math"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/constraint"
	"go.thesmos.sh/sharder/placement/placementtest"
	"go.thesmos.sh/sharder/shard"
)

// TestCapacity checks the validation of a CapacityConfig, NewCapacity and the moves
// that a Capacity rejects.
func TestCapacity(t *testing.T) {
	t.Parallel()

	t.Run("CapacityConfig", func(t *testing.T) {
		t.Parallel()

		t.Run("Validate", func(t *testing.T) {
			t.Parallel()

			valid := []struct {
				name string
				give constraint.CapacityConfig
			}{
				{name: "returns nil for no headroom", give: constraint.CapacityConfig{}},
				{name: "returns nil for a headroom below 1", give: constraint.CapacityConfig{Headroom: 0.999}},
			}
			for _, tt := range valid {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					assert.NoError(t, tt.give.Validate(), "Validate must accept the configuration")
				})
			}

			invalid := []struct {
				name string
				give constraint.CapacityConfig
			}{
				{name: "returns ErrConfig for a negative headroom", give: constraint.CapacityConfig{Headroom: -0.1}},
				{name: "returns ErrConfig for a headroom of 1", give: constraint.CapacityConfig{Headroom: 1}},
				{
					name: "returns ErrConfig for a headroom that is NaN",
					give: constraint.CapacityConfig{Headroom: math.NaN()},
				},
			}
			for _, tt := range invalid {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					err := tt.give.Validate()
					expect.ErrorIs(t, err, constraint.ErrConfig, "Validate must return ErrConfig")
					expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of Validate must classify as Invalid")
				})
			}
		})
	})

	t.Run("NewCapacity", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrConfig for a configuration that Validate refuses", func(t *testing.T) {
			t.Parallel()
			_, err := constraint.NewCapacity(constraint.CapacityConfig{Headroom: 1})
			assert.ErrorIs(t, err, constraint.ErrConfig, "NewCapacity must refuse a headroom of 1")
		})
	})

	t.Run("Name", func(t *testing.T) {
		t.Parallel()

		t.Run("returns CapacityName", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, newCapacity(t, 0).Name(), constraint.CapacityName, "Name must return the registered name")
		})
	})

	t.Run("Check", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name     string
			headroom float64
			give     placement.Move
			want     error
		}{
			{
				name: "returns nil for a replica that fits the free capacity",
				give: placement.Move{To: n1, Shard: s4, Role: shard.RolePrimary},
			},
			{
				name: "returns ErrCapacity for a replica above the capacity",
				give: placement.Move{To: n3, Shard: s4, Role: shard.RolePrimary},
				want: constraint.ErrCapacity,
			},
			{
				name:     "returns ErrCapacity for a replica that takes the headroom",
				headroom: 0.4,
				give:     placement.Move{To: n1, Shard: s4, Role: shard.RolePrimary},
				want:     constraint.ErrCapacity,
			},
			{
				name: "returns nil for a replica that keeps the load of the node below the capacity",
				give: placement.Move{From: n3, To: n3, Shard: s2, Role: shard.RolePrimary},
			},
			{
				name:     "returns ErrCapacity for a replica that keeps the load of the node above the capacity",
				headroom: 0.3,
				give:     placement.Move{From: n3, To: n3, Shard: s2, Role: shard.RolePrimary},
				want:     constraint.ErrCapacity,
			},
			{
				name: "returns nil for a replica that fills the free capacity exactly",
				give: placement.Move{From: d1, To: n3, Shard: s3, Role: shard.RolePrimary},
			},
			{
				name: "returns nil for a move that removes a replica",
				give: placement.Move{From: n3, Shard: s2, Role: shard.RolePrimary},
			},
			{
				name: "returns nil for a node without a capacity",
				give: placement.Move{To: n4, Shard: s4, Role: shard.RolePrimary},
			},
			{
				name: "returns nil for a node that the state does not describe",
				give: placement.Move{From: d1, To: d1, Shard: s3, Role: shard.RolePrimary},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				got := newCapacity(t, tt.headroom).Check(newState(t, cell()), tt.give)
				assert.Equal(t, got, tt.want, "Check must return the error of the capacity")
			})
		}

		sums := []struct {
			name         string
			giveCapacity float64
			givePlaced   float64
			giveAdded    float64
			want         error
		}{
			{
				name:         "returns nil for a replica that fills the free capacity up to the rounding of the sum",
				giveCapacity: 0.3,
				givePlaced:   0.1,
				giveAdded:    0.2,
			},
			{
				name:         "returns nil for a replica that exceeds the free capacity by less than a billionth of it",
				giveCapacity: 1000,
				givePlaced:   600,
				giveAdded:    400.0000005,
			},
			{
				name:         "returns ErrCapacity for a replica that exceeds the free capacity by more than a billionth of it",
				giveCapacity: 1000,
				givePlaced:   600,
				giveAdded:    400.000002,
				want:         constraint.ErrCapacity,
			},
		}
		for _, tt := range sums {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				in := &placement.Input{
					ShardLoad: map[shard.ID]shard.Load{
						s1: {placementtest.CPU: tt.givePlaced},
						s2: {placementtest.CPU: tt.giveAdded},
					},
					Model:  placementtest.Replicated,
					Layout: partition.Layout{Shards: []partition.Shard{{ID: s1}, {ID: s2}}},
					Nodes: []shard.Node{
						{ID: n1, Capacity: map[string]float64{placementtest.CPU: tt.giveCapacity}},
					},
					Metrics:     []placement.Metric{{Name: placementtest.CPU}},
					Parallelism: 1,
					Current: shard.Assignment{Placements: map[shard.ID]shard.Placement{
						s1: {Shard: s1, Replicas: []shard.Replica{{Node: n1, Role: shard.RolePrimary}}},
					}},
				}
				add := placement.Move{To: n1, Shard: s2, Role: shard.RolePrimary}
				got := newCapacity(t, 0).Check(newState(t, in), add)
				assert.Equal(t, got, tt.want, "Check must return the error of the capacity")
			})
		}

		t.Run("returns ErrCapacity for a node that a moving replica still loads", func(t *testing.T) {
			t.Parallel()
			st := newState(t, cell())
			assert.NoError(t, st.Apply(placement.Move{From: n3, To: n4, Shard: s2, Role: shard.RolePrimary}),
				"Apply must move s2 off n3")
			assert.ErrorIs(t, newCapacity(t, 0).Check(st, placement.Move{To: n3, Shard: s4, Role: shard.RolePrimary}),
				constraint.ErrCapacity, "Check must count the load of s2 on n3 until its transition ends")
		})

		t.Run("returns nil for a kept replica on a node whose peak load is above the capacity", func(t *testing.T) {
			t.Parallel()
			st := newState(t, cell())
			assert.NoError(t, st.Apply(placement.Move{From: n3, To: n4, Shard: s2, Role: shard.RolePrimary}),
				"Apply must move s2 off n3")
			assert.NoError(t, st.Apply(placement.Move{To: n3, Shard: s4, Role: shard.RolePrimary}),
				"Apply must add s4 to n3")
			assert.NoError(t,
				newCapacity(t, 0).Check(st, placement.Move{From: n3, To: n3, Shard: s6, Role: shard.RoleSecondary}),
				"Check must compare a kept replica with the load of the node once the moves end")
		})

		t.Run("returns nil for a replica back on the node where the pass found it", func(t *testing.T) {
			t.Parallel()
			st := newState(t, cell())
			assert.NoError(t, st.Apply(placement.Move{From: n3, To: n4, Shard: s2, Role: shard.RolePrimary}),
				"Apply must move s2 off n3")
			assert.NoError(t,
				newCapacity(t, 0.1).Check(st, placement.Move{From: n4, To: n3, Shard: s2, Role: shard.RolePrimary}),
				"Check must count the load of s2 on n3 once")
		})
	})
}

// capacitySubject returns the capacity with a headroom of 0.1, the subject of the suite
// of placement.Constraint. It stops h when NewCapacity refuses it.
func capacitySubject(h *placementtest.ConstraintHandle) *constraint.Capacity {
	return newCapacity(h, 0.1)
}

// newCapacity returns the constraint capacity with the headroom headroom, and stops tb
// when NewCapacity refuses it.
func newCapacity(tb assert.TB, headroom float64) *constraint.Capacity {
	tb.Helper()
	c, err := constraint.NewCapacity(constraint.CapacityConfig{Headroom: headroom})
	assert.NoError(tb, err, "NewCapacity must accept the headroom of the test")
	return c
}
