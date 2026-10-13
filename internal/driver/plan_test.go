// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package driver_test

import (
	"context"
	"testing"
	"testing/synctest"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/internal/driver"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/replication"
	"go.thesmos.sh/sharder/shard"
)

// refusingName is the name of the constraint that refuses every move.
const refusingName shard.StrategyName = "refusing"

// planned is the one shard of the cell of the planning passes of the tests.
var planned = shard.ID{ID128: id.ID128{15: 0x51}}

// placer is a placement.Placer whose Place returns its plan and its error.
type placer struct {
	err  error
	plan placement.Plan
}

var _ placement.Placer = (*placer)(nil)

// Place returns the plan and the error of p.
func (p *placer) Place(context.Context, *placement.Input) (placement.Plan, error) {
	return p.plan, p.err
}

// Schedule returns the zero Schedule.
func (*placer) Schedule() placement.Schedule {
	return placement.Schedule{}
}

// refusing is a placement.Constraint that refuses every move with errBroken.
type refusing struct{}

var _ placement.Constraint = refusing{}

// Name returns refusingName.
func (refusing) Name() shard.StrategyName {
	return refusingName
}

// Check returns errBroken.
func (refusing) Check(*placement.State, placement.Move) error {
	return errBroken
}

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
				cs := []placement.Constraint{refusing{}}
				f := newFixture(
					t,
					performing(machine.Plan{Placer: &placer{plan: plan}, Input: in, Constraints: cs, Token: 1}),
				)
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
				f := newFixture(
					t,
					performing(machine.Plan{Placer: &placer{plan: plan, err: errBusy}, Input: input(), Token: 1}),
				)
				assert.Equal(t, f.results(t, 1)[0], machine.Event(machine.Planned{Plan: plan, Err: errBusy, Token: 1}),
					"the run must pass the error of Place with its plan")
			})
		})

		t.Run("passes the error of Validate with the plan of Place", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				plan := placement.Plan{Targets: map[shard.ID]placement.Target{shardA: {Deficit: 1}}}
				f := newFixture(t, performing(machine.Plan{Placer: &placer{plan: plan}, Input: input(), Token: 1}))
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
