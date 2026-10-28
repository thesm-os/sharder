// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package control_test

import (
	"errors"
	"maps"
	"slices"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/internal/control"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// quietConfig is the configuration of the planner of the tests that set it: the goal count,
// without a pass on a change of the membership.
const quietConfig = `{"goals":[{"name":"count","tier":0,"weight":1}],"triggers":{"onMembership":false}}`

// cpuMetric is the name of the load metric of the tests.
const cpuMetric = "cpu"

// otherIdentity is an identity of the node A in a view after the first.
const otherIdentity shard.Identity = "agent-a-renamed"

// The backoffs of the watch of the views with the source of randomness of the tests: half
// of each ceiling.
const (
	firstViewBackoff  = 50 * time.Millisecond
	secondViewBackoff = 100 * time.Millisecond
)

// The errors of the watches of the tests.
var (
	// errOutage is a transient error of a watch.
	errOutage = errs.WithClass(errors.New("test: the membership is unreachable"), errs.Transient)
	// errBroken is an error of a watch that a retry does not fix.
	errBroken = errs.WithClass(errors.New("test: the membership is broken"), errs.Invalid)
)

// TestMembers checks the views of the membership of a scope and the reports of its agents.
func TestMembers(t *testing.T) {
	t.Parallel()

	t.Run("Viewed", func(t *testing.T) {
		t.Parallel()

		t.Run("places every shard at the first view", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.view(t, nodeA, nodeB)
			got := h.writers(t)
			expect.Equal(t, got[nodeA.ID]+got[nodeB.ID], shards, "the first pass must place every shard")
			expect.Length(t, h.plans, 1, "the first view must start one pass")
		})

		t.Run("ignores an item of another watch", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			out := []machine.Effect{marker}
			_, got, ok := h.controller.Step(machine.Viewed{Token: h.views + 1}, out)
			expect.False(t, ok, "the controller must not take an item of another watch")
			expect.Equal(t, got, out, "the controller must ignore an item of another watch")
		})

		t.Run("ignores an item without a token before it serves", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			out := []machine.Effect{marker}
			_, got, ok := h.controller.Step(machine.Viewed{}, out)
			expect.False(t, ok, "the controller must not take an item before it watches the views")
			expect.Equal(t, got, out, "the controller must ignore an item before it watches the views")
		})

		t.Run("watches the views again after a backoff for a transient error", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			failed := h.views
			h.viewFails(t, errOutage)
			assert.Equal(t, h.views, machine.Token(0), "the controller must not watch the views before the backoff")
			h.elapse(t, firstViewBackoff)
			expect.NotEqual(t, h.views, machine.Token(0), "the controller must watch the views again")
			expect.NotEqual(t, h.views, failed, "the new watch must have a new token")
		})

		t.Run("doubles the backoff of each failed watch in a row", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.viewFails(t, errOutage)
			h.elapse(t, firstViewBackoff)
			h.viewFails(t, errOutage)
			assert.NotEmpty(t, h.timers, "the second failure must arm a backoff")
			assert.Equal(t, h.timers[len(h.timers)-1].Duration, secondViewBackoff,
				"the second backoff in a row must have twice the ceiling")
		})

		t.Run("starts the backoff again after a view", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.viewFails(t, errOutage)
			h.elapse(t, firstViewBackoff)
			h.view(t, nodeA)
			h.viewFails(t, errOutage)
			assert.NotEmpty(t, h.timers, "the failure must arm a backoff")
			assert.Equal(t, h.timers[len(h.timers)-1].Duration, firstViewBackoff,
				"a view must start the backoff again at the first ceiling")
		})

		t.Run("fails at an error of a commit that the view starts", func(t *testing.T) {
			t.Parallel()
			h := spare(t, refuseRelease)
			h.reportAll(t)
			h.disrupt(t, disruption(1, nodeC.ID), operator)
			h.view(t, nodeA, nodeB)
			assert.ErrorIs(t, h.err, store.ErrTooLarge,
				"the controller must fail when the release of a reservation does not fit")
		})

		t.Run("fails at an error of the watch that is not transient", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.viewFails(t, errBroken)
			assert.ErrorIs(t, h.err, errBroken, "the controller must fail at a broken membership")
		})

		t.Run("ignores an item of a watch that ended", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			failed := h.views
			h.viewFails(t, errOutage)
			out := []machine.Effect{marker}
			_, got, ok := h.controller.Step(machine.Viewed{Token: failed}, out)
			expect.False(t, ok, "the controller must not take an item of a watch that ended")
			expect.Equal(t, got, out, "the controller must ignore an item of a watch that ended")
		})

		t.Run("starts a pass for a view that ends a node with replicas", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, quiet)
			h.start(t)
			h.view(t, nodeA, nodeB)
			h.view(t, restarted(nodeA), nodeB)
			assert.Length(t, h.plans, 2, "the end of a node with replicas must start a pass")
		})

		t.Run("starts no pass for a node that joins without OnMembership", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, quiet)
			h.start(t)
			h.view(t, nodeA, nodeB)
			h.view(t, nodeA, nodeB, nodeC)
			assert.Length(t, h.plans, 1, "a join must not start a pass without OnMembership")
		})

		t.Run("starts a pass for a node that joins with OnMembership", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.view(t, nodeA, nodeB)
			h.view(t, nodeA, nodeB, nodeC)
			assert.Length(t, h.plans, 2, "a join must start a pass with OnMembership")
		})

		t.Run("starts no pass for a view that changes nothing", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.view(t, nodeA, nodeB)
			h.view(t, nodeA, nodeB)
			assert.Length(t, h.plans, 1, "an unchanged view must not start a pass")
		})

		t.Run("starts no pass for a node that leaves without replicas", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, quiet)
			h.start(t)
			h.view(t, nodeA, nodeB)
			h.view(t, nodeA, nodeB, nodeC)
			h.view(t, nodeA, nodeB)
			assert.Length(t, h.plans, 1, "a node without replicas must not start a pass when it leaves")
		})

		t.Run("starts a pass for a node that leaves with OnMembership", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.paused = true
			h.view(t, nodeA, nodeB, nodeC)
			joined := h.plans[len(h.plans)-1].Token
			h.view(t, nodeA, nodeB)
			h.paused = false
			h.pass(t, joined)
			assert.Length(t, h.plans, 3, "the node that leaves must start a pass after the pass of its join")
		})

		t.Run("starts a pass for a node that restarts with OnMembership", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.paused = true
			h.view(t, nodeA, nodeB, nodeC)
			joined := h.plans[len(h.plans)-1].Token
			h.view(t, nodeA, nodeB, restarted(nodeC))
			h.paused = false
			h.pass(t, joined)
			assert.True(t, len(h.plans) > 2, "the node that restarts must start a pass after the pass of its join")
			assert.Contains(t, h.plans[2].Input.Nodes, restarted(nodeC),
				"the pass after the pass of the join must have the restarted node")
		})

		t.Run("continues the moves off a node that leaves the view", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.answer = func(*protocol.Request) error { return protocol.ErrUnreachable }
			h.disrupt(t, disruption(1, nodeA.ID), operator)
			h.paused = true
			h.view(t, nodeB)
			assert.Equal(t, h.writers(t)[nodeB.ID], shards, "the view must complete the drops on the node that left")
		})
	})

	t.Run("Report", func(t *testing.T) {
		t.Parallel()

		t.Run("acknowledges the report of a live node", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.view(t, nodeA, nodeB)
			gen := h.state(t).Head.Generation
			h.step(t, h.report(&nodeA, 1))
			assert.Equal(t, last[machine.Reply[protocol.Ack]](t, h).Value,
				protocol.Ack{Sequence: 1, Generation: gen, ControllerEpoch: cellTerm},
				"the ack must contain the sequence, the generation of the scope and the term")
		})

		t.Run("keeps the sequence of the last accepted report", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.view(t, nodeA)
			h.step(t, h.report(&nodeA, 2))
			h.step(t, h.report(&nodeA, 1))
			assert.Equal(t, last[machine.Reply[protocol.Ack]](t, h).Value.Sequence, uint64(2),
				"an older report must not replace the last accepted report")
		})

		t.Run("accepts a report from the identity of the node in the last view", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			renamed := nodeA
			renamed.Identity = otherIdentity
			h.view(t, renamed, nodeB)
			h.step(t, h.report(&renamed, 1))
			assert.Nil(t, last[machine.Reply[protocol.Ack]](t, h).Err,
				"the controller must accept the report of the identity that the last view has")
		})

		t.Run("counts each report that it accepts", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			for _, seq := range []uint64{2, 1, 2} {
				h.step(t, h.report(&nodeA, seq))
			}
			h.elapse(t, interval)
			assert.Equal(t, h.plans[len(h.plans)-1].Input.Snapshot.Reports, uint64(1),
				"the controller must not count a report that is not newer than the last one")
		})

		t.Run("starts no pass for a report without a fault", func(t *testing.T) {
			t.Parallel()
			h := served(t, quiet)
			h.step(t, h.report(&nodeA, 1, healthy(h.shardOn(t, nodeA.ID))))
			h.run(t)
			assert.Length(t, h.plans, 1, "a report without a fault must not start a pass")
		})

		t.Run("passes no fault of a replica without a fault to the next pass", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.step(t, h.report(&nodeA, 1, healthy(h.shardOn(t, nodeA.ID))))
			h.elapse(t, interval)
			assert.Empty(t, h.plans[len(h.plans)-1].Input.Faults, "the pass must not have a fault")
		})

		t.Run("passes no load of a replica that reports none to the next pass", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.step(t, h.report(&nodeA, 1, healthy(h.shardOn(t, nodeA.ID))))
			h.elapse(t, interval)
			assert.Empty(t, h.plans[len(h.plans)-1].Input.ShardLoad, "the pass must not have a load")
		})

		t.Run("accepts a report of any caller without authentication", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.cfg.Unauthenticated = true })
			h.start(t)
			h.view(t, nodeA)
			ev := h.report(&nodeA, 1)
			ev.Metadata.Caller = unknownCaller
			h.step(t, ev)
			assert.Nil(t, last[machine.Reply[protocol.Ack]](t, h).Err,
				"the controller must accept the report of any caller without authentication")
		})

		tests := []struct {
			want error
			give func(*machine.Report)
			name string
		}{
			{
				name: "returns an error that wraps protocol.ErrUnsupported for another version",
				give: func(r *machine.Report) { r.Metadata.Version = protocol.Current + 1 },
				want: protocol.ErrUnsupported,
			},
			{
				name: "returns ErrCall for a report of another scope",
				give: func(r *machine.Report) { r.Report.Scope.Cell = otherCell },
				want: control.ErrCall,
			},
			{
				name: "returns shard.ErrIncarnation for a report of another namespace incarnation",
				give: func(r *machine.Report) { r.Report.Incarnation = shard.NamespaceIncarnation{} },
				want: shard.ErrIncarnation,
			},
			{
				name: "returns ErrStale for a node that the view does not have",
				give: func(r *machine.Report) { r.Report.Node = nodeC.ID },
				want: control.ErrStale,
			},
			{
				name: "returns ErrStale for another incarnation of the node",
				give: func(r *machine.Report) { r.Report.NodeIncarnation++ },
				want: control.ErrStale,
			},
			{
				name: "returns ErrDenied for a caller that is not the node",
				give: func(r *machine.Report) { r.Metadata.Caller = unknownCaller },
				want: control.ErrDenied,
			},
			{
				name: "returns ErrCall for a replica with a status that is not valid",
				give: func(r *machine.Report) {
					r.Report.Replicas = []shard.Observed{{Status: shard.Status(9)}}
				},
				want: control.ErrCall,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				h := newHarness(t)
				h.start(t)
				h.view(t, nodeA)
				ev := h.report(&nodeA, 1)
				tt.give(&ev)
				h.step(t, ev)
				assert.ErrorIs(t, last[machine.Reply[protocol.Ack]](t, h).Err, tt.want,
					"the controller must refuse a report that it cannot accept")
			})
		}

		t.Run("returns ErrNotServing before the controller serves", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.step(t, h.report(&nodeA, 1))
			assert.ErrorIs(t, last[machine.Reply[protocol.Ack]](t, h).Err, control.ErrNotServing,
				"the controller must refuse a report before it serves")
		})

		t.Run("passes the load of a replica in a report to the next pass", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			sid := h.shardOn(t, nodeA.ID)
			load := shard.Load{cpuMetric: 2}
			replica := shard.Observed{Shard: sid, Role: shard.RolePrimary, Status: shard.StatusServing, Load: load}
			h.step(t, h.report(&nodeA, 1, replica))
			h.elapse(t, interval)
			assert.Equal(t, h.plans[len(h.plans)-1].Input.ShardLoad, map[shard.ID]shard.Load{sid: load},
				"the pass must have the load that the writer of the shard reports")
		})

		t.Run("starts a pass for a new permanent fault", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, quiet)
			h.start(t)
			h.view(t, nodeA, nodeB)
			h.step(t, h.report(&nodeA, 1, faulty(h.shardOn(t, nodeA.ID))))
			h.run(t)
			assert.Length(t, h.plans, 2, "a new permanent fault must start a pass")
		})

		t.Run("starts no pass for a fault that the node reported before", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, quiet)
			h.start(t)
			h.view(t, nodeA, nodeB)
			faulted := faulty(h.shardOn(t, nodeA.ID))
			h.step(t, h.report(&nodeA, 1, faulted))
			h.run(t)
			h.step(t, h.report(&nodeA, 2, faulted))
			h.run(t)
			assert.Length(t, h.plans, 2, "a fault that the node reported before must not start another pass")
		})
	})

	t.Run("Called", func(t *testing.T) {
		t.Parallel()

		t.Run("passes the node of an add that fails definitively as a fault of its shard", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.answer = refuses(nodeA)
			h.view(t, nodeA, nodeB)
			assert.True(t, len(h.plans) > 1, "the moves that end without a writer must start a pass")
			faults := h.plans[1].Input.Faults
			assert.NotEmpty(t, faults, "the second pass must have the faults of the refused adds")
			for sid, nodes := range faults {
				expect.Equal(
					t,
					nodes,
					[]shard.NodeID{nodeA.ID},
					"the node of the refused add must be the fault of "+sid.String(),
				)
			}
		})

		t.Run("places a shard on another node after its add fails definitively", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.answer = refuses(nodeA)
			h.view(t, nodeA, nodeB)
			assert.Equal(t, h.writers(t)[nodeB.ID], shards, "every shard must move to the node that did not refuse it")
		})

		t.Run("sends no add again to the incarnation that refused it", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.answer = refuses(nodeA)
			h.view(t, nodeA)
			assert.Length(t, h.sent, shards, "the controller must send one add of each shard")
		})

		t.Run("places a shard on the next incarnation of a node that refused its add", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.answer = refuses(nodeA)
			h.view(t, nodeA)
			h.answer = nil
			h.view(t, restarted(nodeA))
			assert.Equal(t, h.writers(t)[nodeA.ID], shards, "the next incarnation of the node must take every shard")
		})

		t.Run("places a shard on the node of an add that the view ended", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.pause = func(e machine.Effect) bool {
				cb, ok := e.(machine.Callback)
				return ok && cb.Request.NodeIncarnation == nodeA.Incarnation
			}
			h.view(t, nodeA)
			h.view(t, restarted(nodeA))
			assert.Equal(t, h.writers(t)[nodeA.ID], shards,
				"the add that the view ended must not keep the shard off the next incarnation")
		})

		t.Run("keeps the fault of an add that failed definitively after the shard has a writer", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.answer = refuses(nodeA)
			h.view(t, nodeA, nodeB)
			h.elapse(t, interval)
			assert.True(t, len(h.plans) > 1, "the moves that end without a writer must start a pass")
			first, latest := h.plans[1].Input.Faults, h.plans[len(h.plans)-1].Input.Faults
			assert.NotEmpty(t, first, "the second pass must have the faults of the refused adds")
			for sid, nodes := range first {
				expect.Equal(
					t,
					latest[sid],
					nodes,
					"the periodic pass must have the fault of the refused add of "+sid.String(),
				)
			}
		})

		t.Run("does not make a pass an emergency for the fault of an add that failed definitively", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.answer = refuses(nodeA)
			h.view(t, nodeA, nodeB)
			h.elapse(t, interval)
			assert.False(t, h.plans[len(h.plans)-1].Input.Emergency,
				"a fault of a node without the replica of its shard must not be an emergency")
		})

		t.Run("sends no add again to the incarnation that refused it while the shard has another writer",
			func(t *testing.T) {
				t.Parallel()
				h := served(t)
				h.answer = refuses(nodeC)
				h.view(t, nodeA, nodeB, nodeC)
				for range shards {
					h.elapse(t, interval)
				}
				var added []shard.ID
				for _, r := range h.sent {
					if r.Call == protocol.Add && r.Node == nodeC.ID {
						added = append(added, r.Shard)
					}
				}
				assert.NotEmpty(t, added, "the join must start a move to the node")
				assert.NoDuplicates(t, func() ([]shard.ID, error) { return added, nil },
					"the controller must send one add of each shard to the node that refuses it")
			})

		t.Run("lists a node once for a refused shard that the node reports with a permanent fault",
			func(t *testing.T) {
				t.Parallel()
				h := newHarness(t)
				h.start(t)
				h.answer = refuses(nodeA)
				h.view(t, nodeA, nodeB)
				assert.True(t, len(h.plans) > 1, "the moves that end without a writer must start a pass")
				faults := h.plans[1].Input.Faults
				assert.NotEmpty(t, faults, "the second pass must have the faults of the refused adds")
				sid := slices.MinFunc(slices.Collect(maps.Keys(faults)), shard.ID.Compare)
				h.step(t, h.report(&nodeA, 1, faulty(sid)))
				h.run(t)
				assert.Equal(t, h.plans[len(h.plans)-1].Input.Faults[sid], []shard.NodeID{nodeA.ID},
					"the pass must list the node of a refused add and a reported fault once")
			})
	})
}

// refuses returns an answer that returns errRefused for every add on the incarnation of n,
// and nil for every other callback.
func refuses(n shard.Node) func(r *protocol.Request) error {
	return func(r *protocol.Request) error {
		if r.Call == protocol.Add && r.Node == n.ID && r.NodeIncarnation == n.Incarnation {
			return errRefused
		}
		return nil
	}
}

// quiet sets the planner of the spec to the goal count without a pass on a change of the
// membership.
func quiet(s *setup) {
	s.spec.Placement = planned(quietConfig)
}

// report returns a report of the live incarnation of n with the sequence seq and the
// replicas replicas, from the identity of n, as the next inbound call.
func (h *harness) report(n *shard.Node, seq uint64, replicas ...shard.Observed) machine.Report {
	return machine.Report{
		Report: &protocol.Report{
			Scope:           cellScope,
			Node:            n.ID,
			Replicas:        replicas,
			NodeIncarnation: n.Incarnation,
			Sequence:        seq,
			Incarnation:     incA,
		},
		Metadata: protocol.Metadata{Caller: n.Identity, Version: protocol.Current},
		Reply:    h.call(),
	}
}

// faulty returns a serving replica of the shard sid with a permanent fault.
func faulty(sid shard.ID) shard.Observed {
	return shard.Observed{Shard: sid, Role: shard.RolePrimary, Status: shard.StatusServing, Fault: shard.FaultPermanent}
}

// healthy returns a serving replica of the shard sid without a fault and a load.
func healthy(sid shard.ID) shard.Observed {
	return shard.Observed{Shard: sid, Role: shard.RolePrimary, Status: shard.StatusServing}
}

// shardOn returns a shard whose writer is on the node n.
func (h *harness) shardOn(tb assert.TB, n shard.NodeID) shard.ID {
	tb.Helper()
	for _, sid := range h.shardIDs(tb) {
		for _, r := range h.placements(tb)[sid].Replicas {
			if r.Node == n {
				return sid
			}
		}
	}
	tb.Fatalf("no shard has its writer on the node %s", n)
	return shard.ID{}
}
