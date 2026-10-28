// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package driver_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/internal/driver"
	"go.thesmos.sh/sharder/internal/driver/drivertest"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/protocol/protocoltest"
	"go.thesmos.sh/sharder/shard"
)

// timeout is the Timeout of the calls of the tests that bound a call.
const timeout = 5 * time.Second

// The names of the cell and of the metric of the calls of the tests.
const (
	// cellID is the cell of the calls.
	cellID shard.CellID = "cell-a"
	// controller is the address of the controller of the cell.
	controller = "controller-a:7000"
	// metric is the metric of the loads of the reports.
	metric = "cpu"
)

// The records of the calls of the tests to the controller of a cell.
var (
	// cellScope is the scope of the cell of the calls.
	cellScope = shard.Scope{Namespace: namespace, Cell: cellID}
	// cell is the record of the cell of the calls.
	cell = shard.Cell{ID: cellID, Controller: controller}
	// load is the load that the load reporter of the tests reports for shardA.
	load = shard.Load{metric: 2}
	// shardA and shardB are the shards of the replicas of the report of the tests.
	shardA = shard.ID{ID128: id.ID128{15: 0xa}}
	shardB = shard.ID{ID128: id.ID128{15: 0xb}}
)

// TestProtocol checks the effects of the protocol that Run performs: Callback,
// SendReport, SendDisrupt and SendDone.
func TestProtocol(t *testing.T) {
	t.Parallel()

	t.Run("Run", func(t *testing.T) {
		t.Parallel()

		t.Run("passes the node and the request of a Callback to the agent client", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				r := &protocol.Request{Call: protocol.Add}
				f := newFixture(t, performing(machine.Callback{Node: &node, Request: r, Token: 1}))
				var gotNode *shard.Node
				var gotRequest *protocol.Request
				f.cfg.Agents = protocoltest.NewAgentClientStub(t,
					protocoltest.WithAgentClientDo(func(_ context.Context, n *shard.Node, req *protocol.Request) error {
						gotNode, gotRequest = n, req
						return errBusy
					}))
				got, ok := f.results(t, 1)[0].(machine.Called)
				assert.True(t, ok, "the run must pass a Called")
				expect.That(t, got.Err).ErrorIs(errBusy, "the Called must have the error of the agent client")
				expect.Equal(t, got.Token, machine.Token(1), "the Called must have the token of the Callback")
				expect.Equal(t, gotNode, &node, "the run must call the node of the Callback", assert.ByIdentity())
				expect.Equal(t, gotRequest, r, "the run must send the request of the Callback", assert.ByIdentity())
			})
		})

		t.Run("ends a Callback at its Timeout", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(
					t,
					performing(machine.Callback{Node: &node, Request: &protocol.Request{}, Token: 1, Timeout: timeout}),
				)
				f.cfg.Agents = protocoltest.NewAgentClientStub(t,
					protocoltest.WithAgentClientDo(func(ctx context.Context, _ *shard.Node, _ *protocol.Request) error {
						return blocked(ctx)
					}))
				got, ok := f.results(t, 1)[0].(machine.Called)
				assert.True(t, ok, "the run must pass a Called")
				assert.ErrorIs(t, got.Err, context.DeadlineExceeded, "the Called must have the error of the ended call")
			})
		})

		t.Run("passes ErrConfig for a Callback without an agent client", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Callback{Node: &node, Request: &protocol.Request{}, Token: 1}))
				got, ok := f.results(t, 1)[0].(machine.Called)
				assert.True(t, ok, "the run must pass a Called")
				expect.That(t, got.Err).ErrorIs(driver.ErrConfig, "the Called must have ErrConfig")
				expect.Equal(t, got.Token, machine.Token(1), "the Called must have the token of the Callback")
			})
		})

		t.Run("passes the ack of the controller client for a SendReport", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				r := &protocol.Report{Sequence: 4}
				f := newFixture(t, performing(machine.SendReport{Cell: &cell, Report: r, Token: 1}))
				var gotCell *shard.Cell
				var gotReport *protocol.Report
				f.cfg.Controllers = protocoltest.NewControllerClientStub(t, protocoltest.WithControllerClientReport(
					func(_ context.Context, c *shard.Cell, rep *protocol.Report) (protocol.Ack, error) {
						gotCell, gotReport = c, rep
						return protocol.Ack{Sequence: 4}, errBusy
					}))
				got, ok := f.results(t, 1)[0].(machine.Reported)
				assert.True(t, ok, "the run must pass a Reported")
				expect.That(t, got.Err).ErrorIs(errBusy, "the Reported must have the error of the controller client")
				expect.Equal(
					t,
					got.Ack,
					protocol.Ack{Sequence: 4},
					"the Reported must have the ack of the controller client",
				)
				expect.Equal(t, got.Token, machine.Token(1), "the Reported must have the token of the SendReport")
				expect.Equal(t, gotCell, &cell, "the run must call the controller of the cell", assert.ByIdentity())
				expect.Equal(t, gotReport, r, "the run must send the report of the machine", assert.ByIdentity())
			})
		})

		t.Run("sends a report with the loads that the load reporter reports", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				r := &protocol.Report{Sequence: 4, Replicas: []shard.Observed{{Shard: shardA}, {Shard: shardB}}}
				f := newFixture(t, performing(machine.SendReport{Cell: &cell, Report: r, Token: 1}))
				var gotReport *protocol.Report
				f.cfg.Controllers = protocoltest.NewControllerClientStub(t, protocoltest.WithControllerClientReport(
					func(_ context.Context, _ *shard.Cell, rep *protocol.Report) (protocol.Ack, error) {
						gotReport = rep
						return protocol.Ack{}, nil
					}))
				f.cfg.Loads = drivertest.NewLoadReporterStub(t,
					drivertest.WithLoadReporterShardLoad(func(id shard.ID) shard.Load {
						if id == shardA {
							return load
						}
						return nil
					}))
				f.results(t, 1)
				assert.Equal(t, gotReport, &protocol.Report{
					Sequence: 4,
					Replicas: []shard.Observed{{Shard: shardA, Load: load}, {Shard: shardB}},
				}, "the run must send the report with the load of each replica")
				assert.Equal(t, r, &protocol.Report{
					Sequence: 4,
					Replicas: []shard.Observed{{Shard: shardA}, {Shard: shardB}},
				}, "the run must not modify the report of the machine")
			})
		})

		t.Run("ends a SendReport at its Timeout", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(
					t,
					performing(machine.SendReport{Cell: &cell, Report: &protocol.Report{}, Token: 1, Timeout: timeout}),
				)
				f.cfg.Controllers = protocoltest.NewControllerClientStub(t, protocoltest.WithControllerClientReport(
					func(ctx context.Context, _ *shard.Cell, _ *protocol.Report) (protocol.Ack, error) {
						return protocol.Ack{}, blocked(ctx)
					}))
				got, ok := f.results(t, 1)[0].(machine.Reported)
				assert.True(t, ok, "the run must pass a Reported")
				assert.ErrorIs(
					t,
					got.Err,
					context.DeadlineExceeded,
					"the Reported must have the error of the ended call",
				)
			})
		})

		t.Run("passes ErrConfig for a SendReport without a controller client", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.SendReport{Cell: &cell, Report: &protocol.Report{}, Token: 1}))
				got, ok := f.results(t, 1)[0].(machine.Reported)
				assert.True(t, ok, "the run must pass a Reported")
				expect.That(t, got.Err).ErrorIs(driver.ErrConfig, "the Reported must have ErrConfig")
				expect.Equal(t, got.Token, machine.Token(1), "the Reported must have the token of the SendReport")
			})
		})

		t.Run("passes the decision of the controller client for a SendDisrupt", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				d := &budget.Disruption{Scope: cellScope}
				f := newFixture(t, performing(machine.SendDisrupt{Cell: &cell, Disruption: d, Token: 1}))
				var gotCell *shard.Cell
				var gotDisruption *budget.Disruption
				f.cfg.Controllers = protocoltest.NewControllerClientStub(t, protocoltest.WithControllerClientDisrupt(
					func(_ context.Context, c *shard.Cell, dis *budget.Disruption) (budget.Decision, error) {
						gotCell, gotDisruption = c, dis
						return budget.Decision{Allowed: true}, errBusy
					}))
				got, ok := f.results(t, 1)[0].(machine.Decided)
				assert.True(t, ok, "the run must pass a Decided")
				expect.That(t, got.Err).ErrorIs(errBusy, "the Decided must have the error of the controller client")
				expect.Equal(
					t,
					got.Decision,
					budget.Decision{Allowed: true},
					"the Decided must have the decision of the controller client",
				)
				expect.Equal(t, got.Token, machine.Token(1), "the Decided must have the token of the SendDisrupt")
				expect.Equal(t, gotCell, &cell, "the run must call the controller of the cell", assert.ByIdentity())
				expect.Equal(
					t,
					gotDisruption,
					d,
					"the run must send the disruption of the machine",
					assert.ByIdentity(),
				)
			})
		})

		t.Run("ends a SendDisrupt at its Timeout", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(
					t,
					performing(
						machine.SendDisrupt{Cell: &cell, Disruption: &budget.Disruption{}, Token: 1, Timeout: timeout},
					),
				)
				f.cfg.Controllers = protocoltest.NewControllerClientStub(t, protocoltest.WithControllerClientDisrupt(
					func(ctx context.Context, _ *shard.Cell, _ *budget.Disruption) (budget.Decision, error) {
						return budget.Decision{}, blocked(ctx)
					}))
				got, ok := f.results(t, 1)[0].(machine.Decided)
				assert.True(t, ok, "the run must pass a Decided")
				assert.ErrorIs(
					t,
					got.Err,
					context.DeadlineExceeded,
					"the Decided must have the error of the ended call",
				)
			})
		})

		t.Run("passes ErrConfig for a SendDisrupt without a controller client", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(
					t,
					performing(machine.SendDisrupt{Cell: &cell, Disruption: &budget.Disruption{}, Token: 1}),
				)
				got, ok := f.results(t, 1)[0].(machine.Decided)
				assert.True(t, ok, "the run must pass a Decided")
				expect.That(t, got.Err).ErrorIs(driver.ErrConfig, "the Decided must have ErrConfig")
				expect.Equal(t, got.Token, machine.Token(1), "the Decided must have the token of the SendDisrupt")
			})
		})

		t.Run("passes the error of the controller client for a SendDone", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				a := &protocol.DoneArgs{Scope: cellScope}
				f := newFixture(t, performing(machine.SendDone{Cell: &cell, Args: a, Token: 1}))
				var gotCell *shard.Cell
				var gotArgs *protocol.DoneArgs
				f.cfg.Controllers = protocoltest.NewControllerClientStub(t, protocoltest.WithControllerClientDone(
					func(_ context.Context, c *shard.Cell, args *protocol.DoneArgs) error {
						gotCell, gotArgs = c, args
						return errBusy
					}))
				got, ok := f.results(t, 1)[0].(machine.Ended)
				assert.True(t, ok, "the run must pass an Ended")
				expect.That(t, got.Err).ErrorIs(errBusy, "the Ended must have the error of the controller client")
				expect.Equal(t, got.Token, machine.Token(1), "the Ended must have the token of the SendDone")
				expect.Equal(t, gotCell, &cell, "the run must call the controller of the cell", assert.ByIdentity())
				expect.Equal(t, gotArgs, a, "the run must send the argument of the machine", assert.ByIdentity())
			})
		})

		t.Run("ends a SendDone at its Timeout", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(
					t,
					performing(machine.SendDone{Cell: &cell, Args: &protocol.DoneArgs{}, Token: 1, Timeout: timeout}),
				)
				f.cfg.Controllers = protocoltest.NewControllerClientStub(t, protocoltest.WithControllerClientDone(
					func(ctx context.Context, _ *shard.Cell, _ *protocol.DoneArgs) error {
						return blocked(ctx)
					}))
				got, ok := f.results(t, 1)[0].(machine.Ended)
				assert.True(t, ok, "the run must pass an Ended")
				assert.ErrorIs(t, got.Err, context.DeadlineExceeded, "the Ended must have the error of the ended call")
			})
		})

		t.Run("passes ErrConfig for a SendDone without a controller client", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.SendDone{Cell: &cell, Args: &protocol.DoneArgs{}, Token: 1}))
				got, ok := f.results(t, 1)[0].(machine.Ended)
				assert.True(t, ok, "the run must pass an Ended")
				expect.That(t, got.Err).ErrorIs(driver.ErrConfig, "the Ended must have ErrConfig")
				expect.Equal(t, got.Token, machine.Token(1), "the Ended must have the token of the SendDone")
			})
		})
	})
}
