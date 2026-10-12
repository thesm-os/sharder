// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent_test

import (
	"slices"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/agent"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// The times of the Close of the tests.
const (
	// grace is the time between a Close and its deadline.
	grace = 30 * time.Second
	// retryAfter is the RetryAfter of a deferral of a disruption.
	retryAfter = 2 * time.Second
)

// logDisruption is the log message of a disruption that fails.
const logDisruption = "agent: a disruption of the node failed"

// TestClose checks the Close of an agent: the disruption of the node, the end of its session
// and the end of the reservation.
func TestClose(t *testing.T) {
	t.Parallel()

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrNotServing for a Close before the agent serves", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			to := h.close(t, origin.Add(grace))
			expect.ErrorIs(t, replyTo[struct{}](t, h, to).Err, agent.ErrNotServing, "the agent must serve first")
			expect.Nil(t, h.machine.Err(), "a refused Close must not stop the agent")
		})

		t.Run("returns ErrConfig for a Close without a deadline", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			to := h.close(t, time.Time{})
			expect.ErrorIs(t, replyTo[struct{}](t, h, to).Err, agent.ErrConfig, "a Close needs a deadline")
			expect.Empty(t, h.disrupts, "a refused Close must not ask for a disruption")
		})

		t.Run("returns ErrConfig for a Close whose deadline is now", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			to := h.close(t, origin)
			assert.ErrorIs(t, replyTo[struct{}](t, h, to).Err, agent.ErrConfig, "a Close needs a deadline after now")
		})

		t.Run("asks the controller to admit the disruption of the node until the deadline", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.close(t, origin.Add(grace))
			assert.Length(t, h.disrupts, 1, "the agent must ask for the disruption once")
			got := h.disrupts[0]
			expect.Equal(t, *got.Cell, *record(cellTerm), "the agent must ask the controller of the record")
			d := got.Disruption
			expect.Equal(t, d.Start, origin, "the disruption must start at the Close")
			expect.Equal(t, d.End, origin.Add(grace), "the disruption must end at the deadline")
			expect.Equal(t, d.Scope, cellScope, "the disruption must be of the cell")
			expect.Equal(t, d.Nodes, []shard.NodeID{node.ID}, "the disruption must be of the node")
			expect.Equal(t, d.NodeIncarnation, h.incarnation, "the disruption must name the incarnation of the node")
			expect.Equal(t, d.Incarnation, incA, "the disruption must name the namespace incarnation")
			expect.Equal(t, d.Impact, budget.ProcessRestart, "the disruption must restart the process")
			expect.True(t, d.Negotiable, "the disruption must be negotiable")
			expect.False(t, d.ID.IsZero(), "the disruption must have an ID")
		})

		t.Run("ends the session of the node once the disruption is admitted", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.close(t, origin.Add(grace))
			assert.Equal(t, h.leaves, []machine.Token{h.join}, "the agent must end the session of the node")
		})

		t.Run("ends the reservation of the disruption once the session has ended", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			to := h.close(t, origin.Add(grace))
			h.step(t, machine.SessionEnded{Token: h.join})
			h.run(t)
			assert.Length(t, h.dones, 1, "the agent must end the reservation once")
			want := protocol.DoneArgs{Scope: cellScope, Incarnation: incA, ID: h.disrupts[0].Disruption.ID}
			expect.Equal(t, *h.dones[0].Args, want, "the agent must end the reservation of its disruption")
			expect.NoError(t, replyTo[struct{}](t, h, to).Err, "the Close must succeed")
			expect.Nil(t, h.machine.Err(), "a Close must not stop the agent with an error")
			expect.Empty(t, h.watches, "a Close must end the watch of the cell")
		})

		t.Run("asks again after the RetryAfter of a deferral", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			deferred := false
			h.decide = func(*budget.Disruption) (budget.Decision, error) {
				if deferred {
					return budget.Decision{Allowed: true}, nil
				}
				deferred = true
				return budget.Decision{RetryAfter: retryAfter}, nil
			}
			h.close(t, origin.Add(grace))
			expect.Empty(t, h.leaves, "a deferral must not end the session")
			h.elapse(t, retryAfter)
			expect.Length(t, h.disrupts, 2, "the agent must ask again after the RetryAfter")
			expect.Equal(
				t,
				*h.disrupts[1].Disruption,
				*h.disrupts[0].Disruption,
				"the agent must ask for the same disruption",
			)
		})

		t.Run("asks again after RetryBase for a deferral without a RetryAfter", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.decide = func(*budget.Disruption) (budget.Decision, error) { return budget.Decision{}, nil }
			h.close(t, origin.Add(grace))
			h.elapse(t, agent.RetryBase)
			assert.Length(t, h.disrupts, 2, "the agent must ask again after RetryBase")
		})

		t.Run("asks again after a backoff when a disruption fails", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.decide = func(*budget.Disruption) (budget.Decision, error) {
				return budget.Decision{}, protocol.ErrUnreachable
			}
			h.close(t, origin.Add(grace))
			expect.Contains(t, h.logs.logged(), logDisruption, "the agent must log the failed disruption")
			h.elapse(t, firstBackoff)
			expect.Length(t, h.disrupts, 2, "the agent must ask again after the backoff")
		})

		t.Run("asks again after a backoff when a disruption fails with an admission", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.decide = func(*budget.Disruption) (budget.Decision, error) {
				return budget.Decision{Allowed: true}, protocol.ErrUnreachable
			}
			h.close(t, origin.Add(grace))
			expect.Empty(t, h.leaves, "a disruption that fails must not end the session")
			h.elapse(t, firstBackoff)
			expect.Length(t, h.disrupts, 2, "the agent must ask again after the backoff")
		})

		t.Run("replies once to a Close that ends", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			before := len(h.replies)
			to := h.close(t, origin.Add(grace))
			h.step(t, machine.SessionEnded{Token: h.join})
			h.run(t)
			assert.Equal(t, h.replies[before:], []machine.Effect{machine.Reply[struct{}]{To: to}},
				"the agent must reply to the Close once")
		})

		t.Run("ends the reservation again after a backoff for a transient error", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			failed := false
			h.ending = func(*protocol.DoneArgs) error {
				if failed {
					return nil
				}
				failed = true
				return protocol.ErrUnreachable
			}
			to := h.close(t, origin.Add(grace))
			h.step(t, machine.SessionEnded{Token: h.join})
			h.run(t)
			h.elapse(t, firstBackoff)
			expect.Length(t, h.dones, 2, "the agent must end the reservation again")
			expect.NoError(t, replyTo[struct{}](t, h, to).Err, "the Close must succeed")
		})

		t.Run("ends the Close with the error of the end of the reservation", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.ending = func(*protocol.DoneArgs) error { return errBroken }
			to := h.close(t, origin.Add(grace))
			h.step(t, machine.SessionEnded{Token: h.join})
			h.run(t)
			assert.ErrorIs(t, replyTo[struct{}](t, h, to).Err, errBroken, "the Close must fail with the error")
		})

		t.Run("ends the Close with ErrNotServing when the session ends before the admission", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.decide = func(*budget.Disruption) (budget.Decision, error) {
				return budget.Decision{RetryAfter: retryAfter}, nil
			}
			to := h.close(t, origin.Add(grace))
			h.step(t, machine.SessionEnded{Token: h.join})
			expect.ErrorIs(t, replyTo[struct{}](t, h, to).Err, agent.ErrNotServing,
				"a session that ends before the admission must end the Close")
			expect.Empty(t, h.dones, "the agent must not end a reservation that was not admitted")
		})

		t.Run("ends a Close at once when the node has no session", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.step(t, machine.SessionEnded{Token: h.join})
			to := h.close(t, origin.Add(grace))
			expect.NoError(t, replyTo[struct{}](t, h, to).Err, "a Close without a session must succeed")
			expect.Empty(t, h.disrupts, "a node without a session has no replicas to move")
		})

		t.Run("ends the session without the end of the reservation when the caller stops waiting", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.decide = func(*budget.Disruption) (budget.Decision, error) {
				return budget.Decision{RetryAfter: retryAfter}, nil
			}
			to := h.close(t, origin.Add(grace))
			h.step(t, machine.Cancelled{Reply: to})
			expect.Equal(t, h.leaves, []machine.Token{h.join}, "the agent must end the session of the node")
			expect.Empty(t, h.dones, "the agent must not end the reservation")
			expect.False(t, slices.ContainsFunc(h.replies, func(e machine.Effect) bool {
				return e.(machine.Reply[struct{}]).To == to
			}), "the agent must not reply to a Close that the runtime cancelled")
		})

		t.Run("ends the session once when the caller stops waiting after the admission", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			to := h.close(t, origin.Add(grace))
			h.step(t, machine.Cancelled{Reply: to})
			assert.Equal(t, h.leaves, []machine.Token{h.join}, "the agent must not end the session twice")
		})

		t.Run("replies with ErrNotServing to a callback that waits when the Close ends", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			waiting := h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			h.close(t, origin.Add(grace))
			h.step(t, machine.SessionEnded{Token: h.join})
			h.run(t)
			assert.ErrorIs(t, replyTo[struct{}](t, h, waiting).Err, agent.ErrNotServing,
				"a callback that waits must get ErrNotServing when the agent stops")
		})

		t.Run("replies with ErrNotServing to a Close that waits at Stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.decide = func(*budget.Disruption) (budget.Decision, error) {
				return budget.Decision{RetryAfter: retryAfter}, nil
			}
			to := h.close(t, origin.Add(grace))
			h.step(t, machine.Stopped{})
			assert.ErrorIs(t, replyTo[struct{}](t, h, to).Err, agent.ErrNotServing,
				"a Close that waits must get ErrNotServing when the run ends")
		})

		t.Run("ignores the decision of a disruption that it does not wait for", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Decided{Token: 999, Decision: budget.Decision{Allowed: true}}, out)
			assert.Equal(t, got, out, "the agent must ignore the decision of another disruption")
		})

		t.Run("ignores the end of a reservation that it does not wait for", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Ended{Token: 999}, out)
			assert.Equal(t, got, out, "the agent must ignore the end of another reservation")
		})
	})
}

// close passes a Close that ends at the time deadline to the agent, and runs the agent. It
// returns the token of the call.
func (h *harness) close(tb assert.TB, deadline time.Time) machine.ReplyToken {
	tb.Helper()
	to := h.call()
	h.step(tb, machine.Close{Deadline: deadline, Reply: to})
	h.run(tb)
	return to
}
