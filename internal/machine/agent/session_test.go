// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent_test

import (
	"errors"
	"slices"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/assert/stateful"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"

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

// The weights of the actions of the property of the sessions of an agent that a case takes
// more often than the others.
const (
	// tenures is the weight of the assignment of a shard to the node.
	tenures = 2
	// results is the weight of a result of a pending fence acquisition, handler call or
	// revocation.
	results = 4
	// losses is the weight of the end of the session of the node.
	losses = 2
)

// revocations is the share of the cases of the property of the sessions of an agent in which
// the agent revokes a tenure. It is at most half of the smallest share in three runs of 1,000
// cases. The measured shares were 15.6% to 17.5%.
const revocations = 0.07

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
			h := newHarness(t, stops)
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

		t.Run("revokes the tenure of each token in the order of the shards when the session ends under stop",
			func(t *testing.T) {
				t.Parallel()
				h := newHarness(t, stops)
				h.start(t)
				h.serve(t, shardB, 1, firstEpoch)
				h.serve(t, shardA, 1, firstEpoch)
				h.step(t, machine.SessionEnded{Token: h.join})
				h.run(t)
				assert.Length(t, h.revokes, 2, "the end of the lease must revoke each tenure once")
				expect.Equal(t, h.revokes[0].Fence, h.acquires[1].Fence, "the revocation of shard A must come first")
				expect.Equal(t, h.revokes[1].Fence, h.acquires[0].Fence, "the revocation of shard B must come second")
				expect.Equal(t, h.revokes[0].Timeout, dropTimeout, "a revocation must have the timeout of a drop")
				expect.NotContains(t, h.logged(), logRevocation, "the agent must not log a revocation that succeeds")
			})

		t.Run("revokes no tenure when the session ends under continue-fenced", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.step(t, machine.SessionEnded{Token: h.join})
			h.run(t)
			assert.Empty(t, h.revokes, "the end of the lease must not revoke a tenure under continue-fenced")
		})

		t.Run("revokes a tenure whose add runs at the end of the session once the add returns", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, stops)
			h.start(t)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Handle); return ok }
			h.place(t, shardA, firstEpoch)
			h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			h.step(t, machine.SessionEnded{Token: h.join})
			h.run(t)
			assert.Empty(t, h.revokes, "the revocation must wait for the add of its shard")
			h.release(t)
			assert.Length(t, h.revokes, 1, "the agent must revoke the tenure once its add returns")
			expect.Equal(t, h.revokes[0].Fence, h.acquires[0].Fence, "the revocation must name the tenure of the add")
		})

		t.Run("revokes a served tenure whose repeated add fails after the end of the session", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, stops)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Acquire); return ok }
			again := h.request(protocol.Add, shardA, 1, firstEpoch)
			again.Step = 1
			h.do(t, again)
			h.step(t, machine.SessionEnded{Token: h.join})
			h.release(t)
			assert.Length(t, h.revokes, 1, "the agent must revoke the tenure that its first add started")
			expect.Equal(t, h.revokes[0].Fence, h.acquires[0].Fence, "the revocation must name the served tenure")
		})

		t.Run("revokes no tenure whose fence acquisition runs at the end of the session", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, stops)
			h.start(t)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Acquire); return ok }
			h.place(t, shardA, firstEpoch)
			h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			h.step(t, machine.SessionEnded{Token: h.join})
			h.release(t)
			assert.Empty(t, h.revokes, "the agent must not revoke a tenure whose add has not called the handler")
		})

		t.Run("revokes no tenure whose drop runs at the end of the session", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, stops)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Handle); return ok }
			h.do(t, h.request(protocol.Drop, shardA, 2, firstEpoch))
			h.step(t, machine.SessionEnded{Token: h.join})
			h.release(t)
			assert.Empty(t, h.revokes, "the agent must not revoke a tenure that its drop ends")
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

		t.Run("keeps its contract under every sequence of the ends of its session", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "an agent keeps its contract under every sequence of the ends of its session",
				sessions, prop.Require(labelRevoke, revocations))
		})
	})
}

// sessions runs the steps of the case c over an agent that serves under ownership.Stop or
// ownership.ContinueFenced, as c chooses. Each step does one of these things:
//   - It assigns a shard to the node, moves a shard away, or drops a shard.
//   - It passes the next generation to the agent.
//   - It passes success or a result that c chooses to a pending fence acquisition, handler
//     call or revocation.
//   - It ends the session of the node, or ends a Timer, such as the backoff before the next
//     join.
//
// The model checks the contract of the agent after every step and at the end, as the property
// of an agent does. Every case keeps every action, and the steps end the session of a node
// with tenures more often than that property, so they revoke tenures in more cases.
func sessions(c *prop.Case) {
	m := newModel(c, c.Draw(prop.Boolean(), "stop at the end of the lease"))
	behind := func(struct{}) bool {
		return m.applied < len(m.commits) && !m.stopped &&
			slices.ContainsFunc(m.h.watches, func(w machine.Watch) bool { return w.Scope == cellScope })
	}
	pending := func(struct{}) bool { return len(m.h.pending) > 0 }
	stateful.Steps(c, stateful.Machine[struct{}]{
		Actions: []stateful.Action[struct{}]{
			{Name: "assign", Weight: tenures, Run: func(c *prop.Case, _ int, _ any) { m.assign(c) }},
			{Name: "place", Run: func(c *prop.Case, _ int, _ any) { m.place(c) }},
			{Name: "release", Run: func(c *prop.Case, _ int, _ any) { m.release(c) }},
			{Name: "deliver", Weight: results, Enabled: behind, Run: func(c *prop.Case, _ int, _ any) { m.deliver(c) }},
			{
				Name: "succeed", Weight: results, Enabled: pending,
				Run: func(c *prop.Case, _ int, _ any) { m.finish(c, 0, nil) },
			},
			{Name: "answer", Enabled: pending, Run: func(c *prop.Case, _ int, _ any) { m.answer(c) }},
			{
				Name: "end the session", Weight: losses, Enabled: func(struct{}) bool { return m.live },
				Run: func(c *prop.Case, _ int, _ any) { m.end(c) },
			},
			{
				Name: "elapse", Enabled: func(struct{}) bool { return len(m.h.timers) > 0 },
				Run: func(c *prop.Case, _ int, _ any) {
					m.expire(c, c.Draw(prop.Integer(0, len(m.h.timers)-1), "timer"))
				},
			},
			{
				Name: "complete", Drain: true,
				Enabled: func(s struct{}) bool {
					return pending(s) || behind(s) ||
						slices.ContainsFunc(m.h.timers, func(tm machine.Timer) bool { return tm.Duration < loadWindow })
				},
				Run: func(c *prop.Case, _ int, _ any) { m.complete(c) },
			},
		},
		Invariant: func(c *prop.Case, _ struct{}) { m.invariant(c) },
		Settle:    func(c *prop.Case, _ struct{}) { m.settle(c) },
	}, stateful.Swarm(false))
}
