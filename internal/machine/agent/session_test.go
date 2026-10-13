// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent_test

import (
	"encoding/json"
	"errors"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/agent"
	"go.thesmos.sh/sharder/protocol"
)

// The errors of the joins of the tests.
var (
	// errOutage is a transient error of a join.
	errOutage = errs.WithClass(errors.New("test: the membership is unreachable"), errs.Transient)
	// errRefused is an error of a join that a retry does not fix.
	errRefused = errs.WithClass(errors.New("test: the membership refuses the node"), errs.Invalid)
)

// TestSession checks the sessions of the node of an agent.
func TestSession(t *testing.T) {
	t.Parallel()

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("joins again after a backoff when a join fails with a transient error", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			failed := false
			h.joining = func() error {
				if failed {
					return nil
				}
				failed = true
				return errOutage
			}
			started := h.call()
			h.step(t, machine.Started{Reply: started})
			h.run(t)
			expect.Length(t, h.joins, 1, "the agent must wait for the backoff")
			h.elapse(t, firstBackoff)
			expect.Length(t, h.joins, 2, "the agent must join again after the backoff")
			expect.NoError(t, replyTo[struct{}](t, h, started).Err, "the agent must serve after the second join")
		})

		t.Run("joins again after the first backoff once a join succeeds", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			failed := false
			h.joining = func() error {
				if failed {
					return nil
				}
				failed = true
				return errOutage
			}
			h.start(t)
			h.elapse(t, firstBackoff)
			h.step(t, machine.SessionEnded{Token: h.join})
			h.elapse(t, firstBackoff)
			assert.Length(t, h.joins, 3, "a join that succeeds must reset the backoff")
		})

		t.Run("stops at a join error that is not transient", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.joining = func() error { return errRefused }
			h.start(t)
			assert.ErrorIs(t, h.machine.Err(), errRefused, "the agent must stop at a join that a retry does not fix")
		})

		t.Run("ends no session when an error stops it during the backoff of a join", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.joining = func() error { return errOutage }
			h.start(t)
			h.step(t, machine.Started{Reply: h.call()})
			assert.NotNil(t, h.machine.Err(), "a second Started must stop the agent")
			assert.Empty(t, h.leaves, "the agent must not end a session that did not start")
		})

		t.Run("joins again with a new incarnation after its session ends", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.step(t, machine.SessionEnded{Token: h.join})
			h.elapse(t, firstBackoff)
			expect.Length(t, h.joins, 2, "the agent must join again after its session ends")
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Drop, shardA, 1, firstEpoch)))
			expect.NoError(t, got.Err, "the agent must take a callback of its new incarnation")
		})

		t.Run("refuses a callback of the incarnation before its new session", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			old := h.request(protocol.Drop, shardA, 1, firstEpoch)
			h.step(t, machine.SessionEnded{Token: h.join})
			h.elapse(t, firstBackoff)
			got := replyTo[struct{}](t, h, h.do(t, old))
			assert.ErrorIs(t, got.Err, agent.ErrStale, "the agent must refuse a callback of an earlier incarnation")
		})

		t.Run("numbers the reports of a new incarnation from one", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.step(t, machine.SessionEnded{Token: h.join})
			h.elapse(t, firstBackoff)
			last := h.reports[len(h.reports)-1]
			expect.Equal(t, last.NodeIncarnation, h.incarnation, "the report must name the new incarnation")
			expect.Equal(
				t,
				last.Sequence,
				uint64(1),
				"the first report of the new incarnation must have the sequence one",
			)
		})

		t.Run("revokes every token when the session ends under stop", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) {
				s.spec.Ownership = config.Selection{Config: json.RawMessage(stopOnLoss)}
			})
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.step(t, machine.SessionEnded{Token: h.join})
			_, err := h.machine.Token(shardA)
			assert.ErrorIs(t, err, epoch.ErrFenced, "the end of the lease must revoke the token under stop")
		})

		t.Run("keeps every token when the session ends under continue-fenced", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.step(t, machine.SessionEnded{Token: h.join})
			_, err := h.machine.Token(shardA)
			assert.NoError(t, err, "the end of the lease must keep the token under continue-fenced")
		})

		t.Run("serves when its first session starts after the first snapshot", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Join); return ok }
			started := h.call()
			h.step(t, machine.Started{Reply: started})
			h.run(t)
			expect.Empty(t, h.replies, "the agent must wait for its session")
			h.release(t)
			expect.NoError(t, replyTo[struct{}](t, h, started).Err, "the agent must serve once its session starts")
		})

		t.Run("serves when its first snapshot arrives after the first session", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.pause = func(e machine.Effect) bool { r, ok := e.(machine.Read); return ok && r.Scope == cellScope }
			started := h.call()
			h.step(t, machine.Started{Reply: started})
			h.run(t)
			expect.Empty(t, h.replies, "the agent must wait for the snapshot of its cell")
			h.release(t)
			expect.NoError(t, replyTo[struct{}](t, h, started).Err, "the agent must serve once the snapshot arrives")
		})

		t.Run("joins again when its first session ends before it serves", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.pause = func(e machine.Effect) bool { r, ok := e.(machine.Read); return ok && r.Scope == cellScope }
			started := h.call()
			h.step(t, machine.Started{Reply: started})
			h.run(t)
			h.step(t, machine.SessionEnded{Token: h.join})
			h.release(t)
			expect.Empty(t, h.replies, "the agent must not serve without a session")
			h.elapse(t, firstBackoff)
			expect.NoError(t, replyTo[struct{}](t, h, started).Err, "the agent must serve once it joins again")
		})

		t.Run("ignores the end of a session that is not the session of the node", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.SessionEnded{Token: h.join + 100}, out)
			assert.Equal(t, got, out, "the agent must ignore the end of another session")
		})

		t.Run("ignores the result of a join that it does not wait for", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Joined{Token: h.join + 100, Incarnation: 9}, out)
			assert.Equal(t, got, out, "the agent must ignore the result of another join")
		})
	})
}
