// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent_test

import (
	"slices"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/agent"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// loadWindow is the default load window of a spec, the time between two reports of a node
// whose replicas do not change.
const loadWindow = time.Minute

// logReport is the log message of a report that fails.
const logReport = "agent: a report failed"

// TestReport checks the reports of an agent to the controller of its cell.
func TestReport(t *testing.T) {
	t.Parallel()

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("reports the node once it serves", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			assert.Length(t, h.reports, 1, "the agent must report once it serves")
			want := &protocol.Report{
				Scope: cellScope, Node: node.ID, NodeIncarnation: h.incarnation, Sequence: 1, Incarnation: incA,
			}
			assert.Equal(t, h.reports[0], want, "the first report must describe the node without replicas")
		})

		t.Run("reports a replica that serves", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			want := shard.Observed{
				Reported: origin,
				Epoch:    firstEpoch,
				Shard:    shardA,
				Role:     shard.RolePrimary,
				Status:   shard.StatusServing,
			}
			assert.Equal(t, h.observed(t, shardA), want, "the report must describe the replica that serves")
		})

		t.Run("numbers each report of an incarnation", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			for i, r := range h.reports {
				expect.Equal(t, r.Sequence, uint64(i)+1, "each report must have the next sequence")
			}
		})

		t.Run("sends one report at a time", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.held = func(e machine.Effect) bool { _, ok := e.(machine.SendReport); return ok }
			h.serve(t, shardA, 1, firstEpoch)
			expect.Length(t, h.pending, 1, "the agent must have one report in flight")
			h.release(t)
			expect.Equal(t, h.observed(t, shardA).Status, shard.StatusServing,
				"the next report must describe the replicas after the report in flight")
		})

		t.Run("reports again after a backoff when a report fails", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			failed := false
			h.ack = func(r *protocol.Report) (protocol.Ack, error) {
				if failed {
					return protocol.Ack{Sequence: r.Sequence, ControllerEpoch: cellTerm}, nil
				}
				failed = true
				return protocol.Ack{}, protocol.ErrUnreachable
			}
			h.start(t)
			expect.Length(t, h.reports, 1, "the agent must wait for the backoff")
			expect.Contains(t, h.logs.logged(), logReport, "the agent must log the failed report")
			h.elapse(t, firstBackoff)
			expect.Length(t, h.reports, 2, "the agent must report again after the backoff")
		})

		t.Run("reports again after the first backoff once a report succeeds", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			results := []error{protocol.ErrUnreachable, nil, protocol.ErrUnreachable, nil}
			h.ack = func(r *protocol.Report) (protocol.Ack, error) {
				return protocol.Ack{Sequence: r.Sequence, ControllerEpoch: cellTerm}, results[len(h.reports)-1]
			}
			h.start(t)
			h.elapse(t, firstBackoff)
			h.elapse(t, loadWindow)
			h.elapse(t, firstBackoff)
			assert.Length(t, h.reports, len(results), "a report that succeeds must reset the backoff")
		})

		t.Run("waits for the backoff before it reports a change", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.ack = func(*protocol.Report) (protocol.Ack, error) { return protocol.Ack{}, protocol.ErrUnreachable }
			h.start(t)
			h.place(t, shardA, firstEpoch)
			h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			assert.Length(t, h.reports, 1, "a change during the backoff must wait for the backoff")
		})

		t.Run("raises its controller epoch from the ack of a report", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.ack = func(r *protocol.Report) (protocol.Ack, error) {
				return protocol.Ack{Sequence: r.Sequence, ControllerEpoch: cellTerm + 1}, nil
			}
			h.start(t)
			h.place(t, shardA, firstEpoch)
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch)))
			assert.ErrorIs(t, got.Err, agent.ErrStale, "the ack of a newer controller must refuse the replaced one")
		})

		t.Run("sets the lag to the generations that the agent has not applied", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			gen := h.state(t).Head.Generation
			h.ack = func(r *protocol.Report) (protocol.Ack, error) {
				return protocol.Ack{Sequence: r.Sequence, Generation: gen + 5, ControllerEpoch: cellTerm}, nil
			}
			h.elapse(t, loadWindow)
			assert.Equal(t, h.metrics.values[lagInstrument], 5.0, "the lag must count the generations behind the ack")
		})

		t.Run("sets the lag behind the last generation that the agent has applied", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.commitCell(t, &store.Mutation{})
			h.follow(t)
			i := slices.IndexFunc(h.watches, func(w machine.Watch) bool { return w.Scope == cellScope })
			assert.True(t, i >= 0, "the agent must watch the scope of its cell")
			h.step(t, machine.Watched{Event: store.Event{Generation: 1}, Token: h.watches[i].Token})
			gen := h.state(t).Head.Generation
			h.ack = func(r *protocol.Report) (protocol.Ack, error) {
				return protocol.Ack{Sequence: r.Sequence, Generation: gen + 5, ControllerEpoch: cellTerm}, nil
			}
			h.elapse(t, loadWindow)
			assert.Equal(t, h.metrics.values[lagInstrument], 5.0, "an older generation must not change the lag")
		})

		t.Run("sets the lag to zero for an ack at a generation that the agent has applied", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.metrics.values[lagInstrument] = 9
			h.elapse(t, loadWindow)
			assert.Equal(t, h.metrics.values[lagInstrument], 0.0, "the lag must be zero")
		})

		t.Run("reports again after the load window", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.elapse(t, loadWindow)
			assert.Length(t, h.reports, 2, "the agent must report again after the load window")
		})

		t.Run("reports once the controller of the cell has a record", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.acquire = false })
			h.start(t)
			expect.Empty(t, h.reports, "the agent must not report without a controller")
			h.commitCell(t, &store.Mutation{Cell: record(cellTerm)})
			h.follow(t)
			expect.Length(t, h.reports, 1, "the agent must report to the controller of the record")
		})

		t.Run("reports once the node has a session again", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.step(t, machine.SessionEnded{Token: h.join})
			before := len(h.reports)
			h.unplace(t, shardA, secondEpoch)
			expect.Length(t, h.reports, before, "the agent must not report without a session")
			h.elapse(t, firstBackoff)
			expect.Length(t, h.reports, before+1, "the agent must report once it joins again")
		})

		t.Run("ignores the result of a report that it does not wait for", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Reported{Token: 999}, out)
			assert.Equal(t, got, out, "the agent must ignore the result of another report")
		})
	})
}
