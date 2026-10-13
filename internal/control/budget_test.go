// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package control_test

import (
	"slices"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/handoff"
	"go.thesmos.sh/sharder/internal/control"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// The window of each disruption of the tests: it starts an hour after the start of the
// clock and ends an hour later.
const (
	startAfter = time.Hour
	endAfter   = 2 * time.Hour
)

// tagDisruption is the first byte of the IDs of the disruptions of the tests.
const tagDisruption byte = 0xd1

// TestBudget checks the reservations, the drains and the admissions of the budget of a
// scope.
func TestBudget(t *testing.T) {
	t.Parallel()

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("defers a disruption before every live node has reported", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			got := h.disrupt(t, disruption(1, nodeC.ID), operator)
			assert.Nil(t, got.Err, "the controller must take the disruption")
			expect.False(t, got.Value.Allowed, "the controller must defer a disruption before the reports")
			expect.Equal(t, got.Value.RetryAfter, control.RetryAfter,
				"a deferral must have the RetryAfter of the controller")
			expect.NotEmpty(t, got.Value.Reason, "a deferral must have a reason")
		})

		t.Run("defers a disruption before the first view", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			got := h.disrupt(t, disruption(1, nodeA.ID), operator)
			assert.Nil(t, got.Err, "the controller must take the disruption")
			assert.False(t, got.Value.Allowed, "the controller must defer a disruption before it knows its nodes")
		})

		t.Run("defers a disruption while replicas of the drain roles remain on its nodes", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.reportAll(t)
			got := h.disrupt(t, disruption(1, nodeA.ID), operator)
			assert.Nil(t, got.Err, "the controller must take the disruption")
			expect.False(t, got.Value.Allowed, "the controller must defer a disruption of a node with replicas")
			expect.Equal(t, got.Value.RetryAfter, control.RetryAfter,
				"a deferral must have the RetryAfter of the controller")
		})

		t.Run("defers a disruption of the destination of a transition in flight", func(t *testing.T) {
			t.Parallel()
			h := spare(t)
			h.reportAll(t)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Callback); return ok }
			sid := h.shardOn(t, nodeA.ID)
			p := proposalOf(t, 1, proposal.Move, h.placements(t)[sid].Epoch,
				proposal.MoveArgs{Node: nodeC.ID, Shard: sid})
			assert.Nil(t, h.submit(t, &p, operator).Err, "the controller must start the move to the node")
			got := h.disrupt(t, disruption(1, nodeC.ID), operator)
			assert.Nil(t, got.Err, "the controller must take the disruption")
			expect.False(t, got.Value.Allowed, "the controller must defer a disruption of the destination of a move")
			expect.Equal(t, got.Value.RetryAfter, control.RetryAfter,
				"a deferral must have the RetryAfter of the controller")
		})

		t.Run("restores the drains of its scope", func(t *testing.T) {
			t.Parallel()
			drain := store.Drain{Since: origin, Node: nodeA.ID, Caller: operator}
			h := served(t, func(s *setup) { s.prior = []*store.Mutation{{Drain: []store.Drain{drain}}} })
			expect.Equal(t, h.writers(t)[nodeA.ID], 0, "the first pass must not place a shard on the draining node")
			expect.Equal(t, h.writers(t)[nodeB.ID], shards, "the first pass must place every shard on the other node")
		})

		t.Run("drains every node of a disruption", func(t *testing.T) {
			t.Parallel()
			h := spare(t)
			h.disrupt(t, disruption(1, nodeA.ID, nodeB.ID), operator)
			assert.Equal(t, h.writers(t)[nodeC.ID], shards,
				"the drains must move every writer to the node without a drain")
		})

		t.Run("does not drain a node without an ID", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.view(t, nodeA, nodeB, nameless)
			assert.Empty(t, h.plans[len(h.plans)-1].Input.Draining, "the pass must not drain a node without a drain")
		})

		t.Run("defers a move off a node that a pending reservation covers at maxUnavailable zero", func(t *testing.T) {
			t.Parallel()
			h := served(t, unavailable)
			before := h.writers(t)[nodeA.ID]
			h.disrupt(t, disruption(1, nodeA.ID), operator)
			assert.Equal(t, h.writers(t)[nodeA.ID], before,
				"a pending reservation must not admit the moves off its node")
		})

		t.Run("defers a move off a node whose reservation a commit ends at maxUnavailable zero", func(t *testing.T) {
			t.Parallel()
			h := served(t, unavailable)
			h.reportAll(t)
			before := h.writers(t)[nodeA.ID]
			h.paused = true
			forced := disruption(1, nodeA.ID)
			forced.Negotiable = false
			h.disrupt(t, forced, operator)
			drain := h.plans[len(h.plans)-1].Token
			h.elapse(t, startAfter)
			h.step(t, h.doneCall(forced.ID, operator))
			h.pass(t, drain)
			assert.Equal(t, h.writers(t)[nodeA.ID], before,
				"a reservation that a commit ends must not admit the moves off its node")
		})

		t.Run("defers a move off a node that only the reservation of another node covers", func(t *testing.T) {
			t.Parallel()
			h := spare(t, unavailable)
			h.reportAll(t)
			h.disrupt(t, disruption(1, nodeC.ID), operator)
			h.view(t, nodeA, nodeB, nodeC, nodeD)
			deferred := h.metrics.value(deferredInstrument)
			h.elapse(t, interval)
			expect.NotEqual(t, h.metrics.value(deferredInstrument), deferred, "the budget must defer the rebalance")
			expect.Equal(t, h.writers(t)[nodeD.ID], 0, "the reservation of another node must not admit a move")
		})

		t.Run("admits a disruption while the admitted disruptions cover maxConcurrentNodes nodes", func(t *testing.T) {
			t.Parallel()
			h := spare(t)
			h.reportAll(t)
			h.paused = true
			forced := disruption(1, nodeA.ID)
			forced.Negotiable = false
			h.disrupt(t, forced, operator)
			h.elapse(t, startAfter)
			got := h.disrupt(t, disruption(2, nodeC.ID), operator)
			assert.True(t, got.Value.Allowed, "the budget must admit a disruption at maxConcurrentNodes")
		})

		t.Run("admits a disruption while the admitted disruptions cover nodes without replicas", func(t *testing.T) {
			t.Parallel()
			h := spare(t, func(s *setup) { s.spec.Budget.MaxConcurrentNodes = new(int) })
			h.reportAll(t)
			h.disrupt(t, disruption(1, nodeC.ID), operator)
			got := h.disrupt(t, disruption(2, nodeD.ID), operator)
			assert.True(t, got.Value.Allowed, "a node without replicas must not count against maxConcurrentNodes")
		})

		t.Run("admits a disruption while a pending reservation covers a node with replicas", func(t *testing.T) {
			t.Parallel()
			h := spare(t, func(s *setup) { s.spec.Budget.MaxConcurrentNodes = new(int) })
			h.reportAll(t)
			h.paused = true
			h.disrupt(t, disruption(1, nodeA.ID), operator)
			got := h.disrupt(t, disruption(2, nodeC.ID), operator)
			assert.True(t, got.Value.Allowed, "a pending reservation must not count against maxConcurrentNodes")
		})

		t.Run("admits a disruption while a commit ends the reservation of a node with replicas", func(t *testing.T) {
			t.Parallel()
			h := spare(t, func(s *setup) { s.spec.Budget.MaxConcurrentNodes = new(int) })
			h.reportAll(t)
			h.paused = true
			forced := disruption(1, nodeA.ID)
			forced.Negotiable = false
			h.disrupt(t, forced, operator)
			h.elapse(t, startAfter)
			h.step(t, h.doneCall(forced.ID, operator))
			got := h.disrupt(t, disruption(2, nodeC.ID), operator)
			assert.True(t, got.Value.Allowed,
				"a reservation that a commit ends must not count against maxConcurrentNodes")
		})

		t.Run("arms no Timer for a disruption that it admits at once", func(t *testing.T) {
			t.Parallel()
			h := spare(t)
			h.reportAll(t)
			h.disrupt(t, disruption(1, nodeC.ID), operator)
			assert.NotContains(t, durations(h.timers), endAfter, "an admitted reservation must not have a Timer")
		})

		t.Run("arms one Timer for each pending reservation", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.disrupt(t, disruption(1, nodeA.ID), operator)
			h.disrupt(t, disruption(2, nodeB.ID), operator)
			ends := slices.DeleteFunc(durations(h.timers), func(d time.Duration) bool { return d != endAfter })
			assert.Length(t, ends, 2, "each pending reservation must have one Timer of its end")
		})

		t.Run("ignores the end of a disruption whose reservation has ended", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			d := disruption(1, nodeA.ID)
			h.disrupt(t, d, operator)
			h.done(t, d.ID, operator)
			gen := h.state(t).Head.Generation
			h.elapse(t, endAfter)
			assert.Equal(t, h.state(t).Head.Generation, gen, "the end of an ended reservation must not commit")
		})

		t.Run("ignores the end of a disruption whose reservation a commit ends", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			d := disruption(1, nodeA.ID)
			h.disrupt(t, d, operator)
			before := len(h.commits)
			h.step(t, h.doneCall(d.ID, operator))
			i := slices.IndexFunc(h.timers, func(tm machine.Timer) bool { return tm.Duration == endAfter })
			assert.NotEqual(t, i, -1, "the pending reservation must have a Timer of its end")
			h.step(t, machine.Elapsed{Token: h.timers[i].Token})
			h.run(t)
			assert.Length(t, released(h.commits[before:]), 1, "the controller must end the reservation once")
		})

		t.Run("keeps the reservations of its scope until the first view", func(t *testing.T) {
			t.Parallel()
			p := protocolOf(t)
			_, opt := transfer(t, p.Name(), p.Version(), handoff.Commit)
			r := budget.Reservation{
				Disruption:   disruption(1, nodeC.ID),
				Owner:        operator,
				Admitted:     origin,
				Incarnations: map[shard.NodeID]epoch.Epoch{nodeC.ID: nodeC.Incarnation},
			}
			h := newHarness(t, opt, func(s *setup) {
				s.prior = append(s.prior, &store.Mutation{Reserve: []budget.Reservation{r}})
			})
			h.start(t)
			assert.Length(t, h.state(t).Reservations, 1,
				"the controller must not end a reservation before the first view")
		})

		t.Run("ends a reservation once when two views end its node", func(t *testing.T) {
			t.Parallel()
			h := spare(t)
			h.reportAll(t)
			h.disrupt(t, disruption(1, nodeC.ID), operator)
			before := len(h.commits)
			for range 2 {
				h.revision++
				nodes := []shard.Node{nodeA, nodeB}
				v := membership.View{Scope: cellScope, Nodes: nodes, Revision: h.revision, Incarnation: incA}
				h.step(t, machine.Viewed{View: v, Token: h.views})
			}
			h.run(t)
			assert.Length(t, released(h.commits[before:]), 1, "the controller must end the reservation once")
		})

		t.Run("ends a pending reservation of a node of itself when its node leaves the view", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			d := disruption(1, nodeA.ID)
			d.NodeIncarnation = nodeA.Incarnation
			h.disrupt(t, d, nodeA.Identity)
			h.view(t, nodeB)
			assert.Empty(t, h.state(t).Reservations, "the end of the node must end the pending reservation")
		})

		t.Run("ends an admitted reservation once its node has restarted without replicas", func(t *testing.T) {
			t.Parallel()
			h := spare(t)
			h.reportAll(t)
			h.disrupt(t, disruption(1, nodeC.ID), operator)
			h.view(t, nodeA, nodeB, restarted(nodeC))
			assert.Empty(t, h.state(t).Reservations,
				"the controller must end the reservation of a node that has restarted")
		})

		t.Run("replies to a new disruption once its reservation has applied", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			d := disruption(1, nodeA.ID)
			before := len(h.replies)
			h.step(t, h.disruptCall(&d, operator))
			assert.Length(t, h.replies, before, "the controller must not reply before the reservation applies")
			h.run(t)
			assert.Length(t, h.replies, before+1, "the controller must reply once the reservation applies")
		})

		t.Run("commits a pending reservation with a drain of each of its nodes", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			d := disruption(1, nodeA.ID, nodeC.ID)
			h.disrupt(t, d, operator)
			st := h.state(t)
			assert.Length(t, st.Reservations, 1, "the scope must have the reservation")
			expect.True(t, st.Reservations[0].Admitted.IsZero(), "the reservation must be pending")
			expect.Equal(t, st.Reservations[0].Owner, operator, "the reservation must name its owner")
			expect.Equal(t, st.Reservations[0].Incarnations,
				map[shard.NodeID]epoch.Epoch{nodeA.ID: nodeA.Incarnation, nodeC.ID: 0},
				"the reservation must contain the live incarnation of each node, and zero for a node that is not live")
			expect.Equal(t, drained(st.Drains), []store.DrainKey{
				{Node: nodeA.ID, Disruption: d.ID},
				{Node: nodeC.ID, Disruption: d.ID},
			}, "the scope must have a drain of each node of the disruption")
		})

		t.Run("moves the replicas off the nodes of a pending disruption", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.disrupt(t, disruption(1, nodeA.ID), operator)
			assert.Equal(t, h.writers(t)[nodeA.ID], 0, "the drain must move every writer off the node")
		})

		t.Run("admits a repeated disruption once its nodes are empty", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			d := disruption(1, nodeA.ID)
			h.disrupt(t, d, operator)
			h.reportAll(t)
			got := h.disrupt(t, d, operator)
			expect.True(t, got.Value.Allowed, "the controller must admit the disruption of an empty node")
			assert.Length(t, h.state(t).Reservations, 1, "the scope must have the reservation")
			expect.Equal(t, h.state(t).Reservations[0].Admitted, origin, "the commit must record the admission")
		})

		t.Run("admits a new disruption of an empty node at once", func(t *testing.T) {
			t.Parallel()
			h := spare(t)
			h.reportAll(t)
			got := h.disrupt(t, disruption(1, nodeC.ID), operator)
			expect.True(t, got.Value.Allowed, "the controller must admit the disruption of an empty node at once")
			assert.Length(t, h.state(t).Reservations, 1, "the scope must have the reservation")
			expect.False(t, h.state(t).Reservations[0].Admitted.IsZero(), "the reservation must be admitted")
		})

		t.Run("allows a repeated disruption that it has admitted without a commit", func(t *testing.T) {
			t.Parallel()
			h := spare(t)
			h.reportAll(t)
			d := disruption(1, nodeC.ID)
			h.disrupt(t, d, operator)
			gen := h.state(t).Head.Generation
			got := h.disrupt(t, d, operator)
			expect.True(t, got.Value.Allowed, "the controller must allow an admitted disruption")
			expect.Equal(t, h.state(t).Head.Generation, gen,
				"the controller must not commit for an admitted disruption")
		})

		t.Run("defers a repeated disruption without a commit", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			d := disruption(1, nodeA.ID)
			h.disrupt(t, d, operator)
			gen := h.state(t).Head.Generation
			got := h.disrupt(t, d, operator)
			expect.False(t, got.Value.Allowed, "the controller must defer a disruption before the reports")
			expect.Equal(t, h.state(t).Head.Generation, gen, "the controller must not commit for a deferral")
		})

		t.Run("returns budget.ErrReusedID for another disruption with the ID of a reservation", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			d := disruption(1, nodeA.ID)
			h.disrupt(t, d, operator)
			d.End = d.End.Add(time.Hour)
			got := h.disrupt(t, d, operator)
			assert.ErrorIs(t, got.Err, budget.ErrReusedID,
				"the controller must refuse another disruption with a reserved ID")
		})

		t.Run("defers a repeated disruption whose reservation a commit ends", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			d := disruption(1, nodeA.ID)
			h.disrupt(t, d, operator)
			h.step(t, h.doneCall(d.ID, operator))
			got := h.disrupt(t, d, operator)
			expect.False(t, got.Value.Allowed, "the controller must defer a disruption whose reservation ends")
			expect.Equal(t, got.Value.RetryAfter, control.RetryAfter,
				"a deferral must have the RetryAfter of the controller")
		})

		t.Run("defers a disruption that is not negotiable until its start", func(t *testing.T) {
			t.Parallel()
			h := spare(t)
			h.reportAll(t)
			d := disruption(1, nodeC.ID)
			d.Negotiable = false
			got := h.disrupt(t, d, operator)
			expect.False(t, got.Value.Allowed, "the controller must defer a disruption that is not negotiable")
			got = h.disrupt(t, d, operator)
			expect.False(t, got.Value.Allowed, "the controller must defer it again before its start")
			expect.Equal(t, got.Value.RetryAfter, control.RetryAfter,
				"a deferral must have the RetryAfter of the controller")
		})

		t.Run("admits a disruption that is not negotiable at its start", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			d := disruption(1, nodeA.ID)
			d.Negotiable = false
			h.disrupt(t, d, operator)
			h.clock.Advance(startAfter)
			h.elapse(t, startAfter)
			assert.Length(t, h.state(t).Reservations, 1, "the scope must keep the reservation")
			expect.Equal(t, h.state(t).Reservations[0].Admitted, origin.Add(startAfter),
				"the controller must admit the disruption at its start")
		})

		t.Run("defers a disruption above maxConcurrentNodes", func(t *testing.T) {
			t.Parallel()
			h := spare(t, func(s *setup) { s.spec.Budget.MaxConcurrentNodes = new(int) })
			h.reportAll(t)
			h.paused = true
			forced := disruption(1, nodeA.ID)
			forced.Negotiable = false
			h.disrupt(t, forced, operator)
			h.elapse(t, startAfter)
			got := h.disrupt(t, disruption(2, nodeC.ID), operator)
			expect.False(t, got.Value.Allowed,
				"the controller must defer a disruption while an admitted one covers a node with replicas")
			expect.Equal(t, got.Value.RetryAfter, control.RetryAfter,
				"a deferral must have the RetryAfter of the controller")
		})

		t.Run("ends a pending reservation at the end of its disruption", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.disrupt(t, disruption(1, nodeA.ID), operator)
			h.elapse(t, endAfter)
			st := h.state(t)
			expect.Empty(t, st.Reservations, "the end of the disruption must end its pending reservation")
			expect.Empty(t, st.Drains, "the end of the reservation must end its drains")
		})

		t.Run("keeps an admitted reservation at the end of its disruption", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			d := disruption(1, nodeA.ID)
			h.disrupt(t, d, operator)
			h.reportAll(t)
			h.disrupt(t, d, operator)
			h.elapse(t, endAfter)
			assert.Length(t, h.state(t).Reservations, 1,
				"the end of the disruption must not end an admitted reservation")
		})

		t.Run("ends a pending reservation of a node of itself when its incarnation ends", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			d := disruption(1, nodeA.ID)
			d.NodeIncarnation = nodeA.Incarnation
			h.disrupt(t, d, nodeA.Identity)
			h.view(t, restarted(nodeA), nodeB)
			expect.Empty(t, h.state(t).Reservations, "the end of the incarnation must end the pending reservation")
		})

		t.Run("ends an admitted reservation once its node has left without replicas", func(t *testing.T) {
			t.Parallel()
			h := spare(t)
			h.reportAll(t)
			h.disrupt(t, disruption(1, nodeC.ID), operator)
			h.view(t, nodeA, nodeB)
			expect.Empty(t, h.state(t).Reservations, "the controller must end the reservation of a node that has left")
		})

		t.Run("keeps an admitted reservation while its node is live", func(t *testing.T) {
			t.Parallel()
			h := spare(t)
			h.reportAll(t)
			h.disrupt(t, disruption(1, nodeC.ID), operator)
			h.view(t, nodeA, nodeB, nodeC)
			assert.Length(t, h.state(t).Reservations, 1, "the controller must keep the reservation of a live node")
		})

		t.Run("keeps a pending reservation of an operator when its nodes restart", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.disrupt(t, disruption(1, nodeA.ID), operator)
			h.view(t, restarted(nodeA), nodeB)
			assert.Length(t, h.state(t).Reservations, 1,
				"a restart must not end the pending reservation of an operator")
		})

		t.Run("ignores the end of a disruption that the controller has admitted", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			d := disruption(1, nodeA.ID)
			h.disrupt(t, d, operator)
			h.reportAll(t)
			h.disrupt(t, d, operator)
			gen := h.state(t).Head.Generation
			h.elapse(t, endAfter)
			assert.Equal(t, h.state(t).Head.Generation, gen, "the end of an admitted disruption must not commit")
		})

		t.Run("arms the timers of the pending reservations of its scope", func(t *testing.T) {
			t.Parallel()
			d := disruption(1, nodeA.ID)
			r := budget.Reservation{Disruption: d, Owner: operator}
			h := newHarness(t, func(s *setup) { s.prior = []*store.Mutation{{Reserve: []budget.Reservation{r}}} })
			h.start(t)
			h.elapse(t, endAfter)
			expect.Empty(t, h.state(t).Reservations, "the end of the disruption must end the restored reservation")
		})

		t.Run("counts each deferral", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			d := disruption(1, nodeA.ID)
			h.disrupt(t, d, operator)
			h.disrupt(t, d, operator)
			assert.Equal(t, h.metrics.value(deferredInstrument), 2.0, "the controller must count both deferrals")
		})

		t.Run("accepts a disruption of a node of itself from the node", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			d := disruption(1, nodeA.ID)
			d.NodeIncarnation = nodeA.Incarnation
			got := h.disrupt(t, d, nodeA.Identity)
			assert.Nil(t, got.Err, "the controller must take the disruption of a node of itself")
		})

		t.Run("accepts a disruption from any caller without authentication", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.cfg.Unauthenticated = true })
			h.start(t)
			h.view(t, nodeA, nodeB)
			got := h.disrupt(t, disruption(1, nodeA.ID, nodeB.ID), unknownCaller)
			assert.Nil(t, got.Err, "the controller must take the disruption of any caller without authentication")
		})

		tests := []struct {
			want   error
			give   func(*budget.Disruption)
			name   string
			caller shard.Identity
		}{
			{
				name:   "returns ErrDenied for a caller that is not an operator",
				give:   func(*budget.Disruption) {},
				caller: unknownCaller,
				want:   control.ErrDenied,
			},
			{
				name:   "returns ErrDenied for a node that disrupts another node",
				give:   func(d *budget.Disruption) { d.NodeIncarnation = nodeA.Incarnation },
				caller: nodeB.Identity,
				want:   control.ErrDenied,
			},
			{
				name:   "returns ErrStale for a disruption of a node at another incarnation",
				give:   func(d *budget.Disruption) { d.NodeIncarnation = nodeA.Incarnation + 1 },
				caller: nodeA.Identity,
				want:   control.ErrStale,
			},
			{
				name: "returns ErrStale for a disruption of a node of itself that the view does not have",
				give: func(d *budget.Disruption) {
					d.NodeIncarnation = nodeC.Incarnation
					d.Nodes = []shard.NodeID{nodeC.ID}
				},
				caller: nodeC.Identity,
				want:   control.ErrStale,
			},
			{
				name: "returns ErrCall for a disruption of a node of itself with two nodes",
				give: func(d *budget.Disruption) {
					d.NodeIncarnation = nodeA.Incarnation
					d.Nodes = []shard.NodeID{nodeA.ID, nodeB.ID}
				},
				caller: nodeA.Identity,
				want:   control.ErrCall,
			},
			{
				name:   "returns ErrCall for a disruption without an ID",
				give:   func(d *budget.Disruption) { d.ID = budget.ID{} },
				caller: operator,
				want:   control.ErrCall,
			},
			{
				name:   "returns ErrCall for a disruption without nodes",
				give:   func(d *budget.Disruption) { d.Nodes = nil },
				caller: operator,
				want:   control.ErrCall,
			},
			{
				name:   "returns ErrCall for a disruption that ends at its start",
				give:   func(d *budget.Disruption) { d.End = d.Start },
				caller: operator,
				want:   control.ErrCall,
			},
			{
				name:   "returns ErrCall for a disruption without an impact",
				give:   func(d *budget.Disruption) { d.Impact = 0 },
				caller: operator,
				want:   control.ErrCall,
			},
			{
				name:   "returns budget.ErrUnknownImpact for a disruption with an unknown impact",
				give:   func(d *budget.Disruption) { d.Impact = budget.Impact(9) },
				caller: operator,
				want:   budget.ErrUnknownImpact,
			},
			{
				name:   "returns ErrCall for a disruption of another scope",
				give:   func(d *budget.Disruption) { d.Scope.Cell = otherCell },
				caller: operator,
				want:   control.ErrCall,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				h := served(t)
				d := disruption(1, nodeA.ID)
				tt.give(&d)
				got := h.disrupt(t, d, tt.caller)
				assert.ErrorIs(t, got.Err, tt.want, "the controller must refuse the disruption")
				expect.Empty(t, h.state(t).Reservations, "a refused disruption must not reserve anything")
			})
		}

		t.Run("returns ErrNotServing for a disruption before the controller serves", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			d := disruption(1, nodeA.ID)
			h.step(t, h.disruptCall(&d, operator))
			assert.ErrorIs(t, last[machine.Reply[budget.Decision]](t, h).Err, control.ErrNotServing,
				"the controller must refuse a disruption before it serves")
		})

		t.Run("ends a reservation for its owner once the commit has applied", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			d := disruption(1, nodeA.ID)
			d.NodeIncarnation = nodeA.Incarnation
			h.disrupt(t, d, nodeA.Identity)
			before := len(h.replies)
			h.step(t, h.doneCall(d.ID, nodeA.Identity))
			assert.Length(t, h.replies, before, "the controller must not reply to Done before its commit applies")
			h.run(t)
			assert.Nil(t, last[machine.Reply[struct{}]](t, h).Err,
				"the controller must end the reservation for its owner")
			st := h.state(t)
			expect.Empty(t, st.Reservations, "Done must end the reservation")
			expect.Empty(t, st.Drains, "Done must end the drains of the reservation")
		})

		t.Run("ends a reservation for an operator", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			d := disruption(1, nodeA.ID)
			d.NodeIncarnation = nodeA.Incarnation
			h.disrupt(t, d, nodeA.Identity)
			h.done(t, d.ID, operator)
			assert.Nil(t, last[machine.Reply[struct{}]](t, h).Err,
				"the controller must end the reservation for an operator")
			expect.Empty(t, h.state(t).Reservations, "Done must end the reservation")
		})

		t.Run("ends a reservation for any caller without authentication", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.cfg.Unauthenticated = true })
			h.start(t)
			h.view(t, nodeA, nodeB)
			d := disruption(1, nodeA.ID)
			h.disrupt(t, d, operator)
			h.done(t, d.ID, unknownCaller)
			expect.Empty(t, h.state(t).Reservations, "Done must end the reservation without authentication")
		})

		t.Run("ends a reservation whose commit is in flight", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Commit); return ok }
			d := disruption(1, nodeA.ID)
			h.step(t, h.disruptCall(&d, operator))
			h.step(t, h.doneCall(d.ID, operator))
			h.pause = nil
			h.run(t)
			st := h.state(t)
			expect.Empty(t, st.Reservations, "Done must end a reservation whose commit was in flight")
			expect.Empty(t, st.Drains, "Done must end the drains of the reservation")
		})

		t.Run("returns ErrDenied for a Done of a caller that is neither the owner nor an operator", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			d := disruption(1, nodeA.ID)
			h.disrupt(t, d, operator)
			h.done(t, d.ID, unknownCaller)
			assert.ErrorIs(t, last[machine.Reply[struct{}]](t, h).Err, control.ErrDenied,
				"the controller must refuse Done of a caller that is not authorised")
			assert.Length(t, h.state(t).Reservations, 1, "a refused Done must keep the reservation")
		})

		t.Run("replies at once to a Done of a reservation that has ended", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.step(t, h.doneCall(disruption(1, nodeA.ID).ID, operator))
			assert.Nil(t, last[machine.Reply[struct{}]](t, h).Err, "Done of an ended reservation must succeed")
		})

		t.Run("replies at once to a Done of a reservation that a commit ends", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			d := disruption(1, nodeA.ID)
			h.disrupt(t, d, operator)
			h.step(t, h.doneCall(d.ID, operator))
			second := h.doneCall(d.ID, operator)
			h.step(t, second)
			assert.Nil(t, replyTo[struct{}](t, h, second.Reply).Err, "the second Done must get its reply at once")
		})

		t.Run("returns ErrNotServing for a Done before the controller serves", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.step(t, h.doneCall(disruption(1, nodeA.ID).ID, operator))
			assert.ErrorIs(t, last[machine.Reply[struct{}]](t, h).Err, control.ErrNotServing,
				"the controller must refuse Done before it serves")
		})

		t.Run("replies with ErrNotServing to a Done that waits when the controller stops", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			d := disruption(1, nodeA.ID)
			h.disrupt(t, d, operator)
			h.step(t, h.doneCall(d.ID, operator))
			h.stop(t)
			assert.ErrorIs(t, last[machine.Reply[struct{}]](t, h).Err, control.ErrNotServing,
				"a Done that waits for its commit must get ErrNotServing when the controller stops")
		})

		t.Run("replies with ErrNotServing to a disruption that waits when the controller stops", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			d := disruption(1, nodeA.ID)
			h.step(t, h.disruptCall(&d, operator))
			h.stop(t)
			assert.ErrorIs(t, last[machine.Reply[budget.Decision]](t, h).Err, control.ErrNotServing,
				"a disruption that waits for its commit must get ErrNotServing when the controller stops")
		})
	})
}

// served returns a harness of a controller that serves with a view of the nodes A and B,
// whose first pass has placed every shard. Each option changes the setup.
func served(tb testCase, opts ...option) *harness {
	tb.Helper()
	h := newHarness(tb, opts...)
	h.start(tb)
	h.view(tb, nodeA, nodeB)
	return h
}

// spare returns a harness of a controller whose shards are on the nodes A and B, and whose
// view also has the node C without a replica. The planner of the controller does not start
// a pass for a change of the membership. Each option changes the setup.
func spare(tb testCase, opts ...option) *harness {
	tb.Helper()
	h := served(tb, append(opts, quiet)...)
	h.view(tb, nodeA, nodeB, nodeC)
	return h
}

// disruption returns a negotiable disruption with the ID n of the nodes nodes. It restarts
// the processes of the nodes from an hour after the start of the clock for an hour.
func disruption(n byte, nodes ...shard.NodeID) budget.Disruption {
	return budget.Disruption{
		Start:       origin.Add(startAfter),
		End:         origin.Add(endAfter),
		Scope:       cellScope,
		Nodes:       nodes,
		ID:          budget.ID{ID: id.New128([16]byte{0: tagDisruption, 15: n})},
		Incarnation: incA,
		Impact:      budget.ProcessRestart,
		Negotiable:  true,
	}
}

// disruptCall returns the Disrupt of d from caller as the next inbound call.
func (h *harness) disruptCall(d *budget.Disruption, caller shard.Identity) machine.Disrupt {
	return machine.Disrupt{
		Disruption: d,
		Metadata:   protocol.Metadata{Caller: caller, Version: protocol.Current},
		Reply:      h.call(),
	}
}

// disrupt passes the Disrupt of d from caller and runs the machine. It returns the reply to
// the call.
func (h *harness) disrupt(tb assert.TB, d budget.Disruption, caller shard.Identity) machine.Reply[budget.Decision] {
	tb.Helper()
	call := h.disruptCall(&d, caller)
	h.step(tb, call)
	h.run(tb)
	return replyTo[budget.Decision](tb, h, call.Reply)
}

// doneCall returns the Done of the reservation of the disruption id from caller as the
// next inbound call.
func (h *harness) doneCall(id budget.ID, caller shard.Identity) machine.Done {
	return machine.Done{
		Args:     &protocol.DoneArgs{Scope: cellScope, Incarnation: incA, ID: id},
		Metadata: protocol.Metadata{Caller: caller, Version: protocol.Current},
		Reply:    h.call(),
	}
}

// done passes the Done of the reservation of the disruption id from caller, and runs the
// machine.
func (h *harness) done(tb assert.TB, id budget.ID, caller shard.Identity) {
	tb.Helper()
	h.step(tb, h.doneCall(id, caller))
	h.run(tb)
}

// reportAll passes a report of every node of the last view, each with the next sequence.
func (h *harness) reportAll(tb assert.TB) {
	tb.Helper()
	for _, n := range h.nodes {
		h.sequence++
		h.step(tb, h.report(&n, h.sequence))
		assert.Nil(tb, last[machine.Reply[protocol.Ack]](tb, h).Err, "the controller must accept the report")
	}
	h.run(tb)
}

// drained returns the key of each drain of ds.
func drained(ds []store.Drain) []store.DrainKey {
	keys := make([]store.DrainKey, 0, len(ds))
	for _, d := range ds {
		keys = append(keys, store.DrainKey{Node: d.Node, Disruption: d.Disruption})
	}
	return keys
}

// released returns the IDs of the reservations that the mutations ms end, in order.
func released(ms []*store.Mutation) []budget.ID {
	var ids []budget.ID
	for _, m := range ms {
		ids = append(ids, m.Release...)
	}
	return ids
}
