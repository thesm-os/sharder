// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package driver_test

import (
	"testing"
	"testing/synctest"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/internal/driver"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/placementtest"
	"go.thesmos.sh/sharder/replication"
	"go.thesmos.sh/sharder/shard"
)

// refusingName is the name of the constraint that refuses every move.
const refusingName shard.StrategyName = "refusing"

// planned is the one shard of the cell of the planning passes of the tests.
var planned = shard.ID{ID128: id.ID128{15: 0x51}}

// input returns the input of the planning passes of the tests: a cell of the model
// primary-only with one node and one shard, whose writer is on the node.
func input() *placement.Input {
	return &placement.Input{
		Model:  replication.PrimaryOnly,
		Layout: partition.Layout{Cell: cellID, Shards: []partition.Shard{{ID: planned}}},
		Current: shard.Assignment{Placements: map[shard.ID]shard.Placement{
			planned: {Shard: planned, Replicas: []shard.Replica{{Node: node.ID, Role: shard.RolePrimary}}},
		}},
		Nodes: []shard.Node{{ID: node.ID}},
	}
}

// TestPlan checks the effect of a planning pass that Run performs: Plan.
func TestPlan(t *testing.T) {
	t.Parallel()

	t.Run("Run", func(t *testing.T) {
		t.Parallel()

		t.Run("passes the plan of Place and the violations of Validate for a Plan", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				plan := placement.Plan{RepairNext: planned}
				in := input()
				refusing := placementtest.NewConstraintStub(t)
				refusing.OnName.Returns(refusingName)
				refusing.OnCheck.Returns(errBroken)
				cs := []placement.Constraint{refusing}
				p := placementtest.NewPlacerStub(t)
				p.OnPlace.Returns(plan, nil)
				f := newFixture(t, performing(machine.Plan{Placer: p, Input: in, Constraints: cs, Token: 1}))
				violations, err := placement.Validate(input(), plan, cs)
				assert.NoError(t, err, "Validate accepts the plan")
				assert.NotEmpty(t, violations, "the constraint refuses the replica that the plan keeps")
				assert.Equal(
					t,
					f.results(t, 1)[0],
					machine.Event(machine.Planned{Plan: plan, Violations: violations, Token: 1}),
					"the run must pass the plan and its violations",
				)
			})
		})

		t.Run("passes the error of Place with its plan", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				plan := placement.Plan{Incomplete: true}
				p := placementtest.NewPlacerStub(t)
				p.OnPlace.Returns(plan, errBusy)
				f := newFixture(t, performing(machine.Plan{Placer: p, Input: input(), Token: 1}))
				assert.Equal(t, f.results(t, 1)[0], machine.Event(machine.Planned{Plan: plan, Err: errBusy, Token: 1}),
					"the run must pass the error of Place with its plan")
			})
		})

		t.Run("passes the error of Validate with the plan of Place", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				plan := placement.Plan{Targets: map[shard.ID]placement.Target{shardA: {Deficit: 1}}}
				p := placementtest.NewPlacerStub(t)
				p.OnPlace.Returns(plan, nil)
				f := newFixture(t, performing(machine.Plan{Placer: p, Input: input(), Token: 1}))
				got, ok := f.results(t, 1)[0].(machine.Planned)
				assert.True(t, ok, "the run must pass a Planned")
				expect.That(t, got.Err).ErrorIs(placement.ErrPlan, "the Planned must have the error of Validate")
				expect.Equal(t, got, machine.Planned{Plan: plan, Err: got.Err, Token: 1},
					"the Planned must have the plan of Place and no violation")
			})
		})

		t.Run("passes ErrConfig for a Plan without a placer", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Plan{Input: input(), Token: 1}))
				got, ok := f.results(t, 1)[0].(machine.Planned)
				assert.True(t, ok, "the run must pass a Planned")
				expect.That(t, got.Err).ErrorIs(driver.ErrConfig, "the Planned must have ErrConfig")
				expect.Equal(t, got.Token, machine.Token(1), "the Planned must have the token of the Plan")
			})
		})
	})
}
