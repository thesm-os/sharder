// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package control_test

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/handoff"
	"go.thesmos.sh/sharder/internal/control"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/constraint"
	"go.thesmos.sh/sharder/placement/goal"
	"go.thesmos.sh/sharder/placement/planner"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// tagProposal is the first byte of the IDs of the proposals of the tests.
const tagProposal byte = 0x9e

// expiry is the time after the start of the clock at which the proposals of the tests
// expire.
const expiry = time.Hour

// unknownKey is a key that the arguments of a drain do not declare.
const unknownKey = "nodes"

// tieredConfig is the configuration of the planner of the tests that set it: the goal
// count in tier 1 with the weight 2, and twice in tier 0 with the weights 1 and 3, without
// a pass on a change of the membership.
const tieredConfig = `{"goals":[{"name":"count","tier":1,"weight":2},{"name":"count","tier":0,"weight":1},` +
	`{"name":"count","tier":0,"weight":3}],"triggers":{"onMembership":false}}`

// TestProposal checks the proposals that the operators of a scope submit.
func TestProposal(t *testing.T) {
	t.Parallel()

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("drains a node with one commit that records the proposal", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			p := proposalOf(t, 1, proposal.Drain, nodeA.Incarnation, proposal.NodeArgs{Node: nodeA.ID})
			got := h.submit(t, &p, operator)
			assert.Nil(t, got.Err, "the controller must apply the drain")
			expect.True(t, got.Value.Applied, "the receipt must report that the controller applied the drain")
			expect.Equal(t, got.Value.Incarnation, incA, "the receipt must contain the namespace incarnation")
			st := h.state(t)
			expect.Equal(t, drained(st.Drains), []store.DrainKey{{Node: nodeA.ID}},
				"the scope must have the manual drain")
			expect.Contains(t, slices.Collect(maps.Keys(st.Applied)), p.ID, "the scope must record the proposal")
			expect.Equal(t, h.writers(t)[nodeA.ID], 0, "the drain must move every writer off the node")
		})

		t.Run("returns the decision of the budget for the node in the receipt of a drain", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			p := proposalOf(t, 1, proposal.Drain, nodeA.Incarnation, proposal.NodeArgs{Node: nodeA.ID})
			got := h.submit(t, &p, operator)
			var decision budget.Decision
			assert.NoError(t, json.Unmarshal(got.Value.Result, &decision), "the result of a drain must be a decision")
			expect.False(t, decision.Allowed, "the budget must defer the disruption of a node before the reports")
			expect.Equal(t, decision.RetryAfter, control.RetryAfter,
				"the decision must have the RetryAfter of the controller")
		})

		t.Run("keeps the manual drain of a node that has one", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			first := proposalOf(t, 1, proposal.Drain, nodeA.Incarnation, proposal.NodeArgs{Node: nodeA.ID})
			h.submit(t, &first, operator)
			second := proposalOf(t, 2, proposal.Drain, nodeA.Incarnation, proposal.NodeArgs{Node: nodeA.ID})
			got := h.submit(t, &second, operator)
			assert.Nil(t, got.Err, "the controller must apply the second drain")
			assert.Empty(t, h.commits[len(h.commits)-1].Drain, "a second drain must not replace the manual drain")
		})

		t.Run("undrains a node with one commit that records the proposal", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			drain := proposalOf(t, 1, proposal.Drain, nodeA.Incarnation, proposal.NodeArgs{Node: nodeA.ID})
			h.submit(t, &drain, operator)
			undrain := proposalOf(t, 2, proposal.Undrain, nodeA.Incarnation, proposal.NodeArgs{Node: nodeA.ID})
			got := h.submit(t, &undrain, operator)
			assert.Nil(t, got.Err, "the controller must apply the undrain")
			st := h.state(t)
			expect.Empty(t, st.Drains, "the undrain must remove the manual drain")
			expect.Contains(t, slices.Collect(maps.Keys(st.Applied)), undrain.ID, "the scope must record the undrain")
			expect.Empty(t, h.plans[len(h.plans)-1].Input.Draining,
				"the pass after the undrain must not drain the node")
		})

		t.Run("drains a node again while the commit of its undrain is in flight", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			first := proposalOf(t, 1, proposal.Drain, nodeA.Incarnation, proposal.NodeArgs{Node: nodeA.ID})
			h.submit(t, &first, operator)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Commit); return ok }
			undrain := proposalOf(t, 2, proposal.Undrain, nodeA.Incarnation, proposal.NodeArgs{Node: nodeA.ID})
			h.step(t, h.submitCall(&undrain, operator))
			again := proposalOf(t, 3, proposal.Drain, nodeA.Incarnation, proposal.NodeArgs{Node: nodeA.ID})
			h.step(t, h.submitCall(&again, operator))
			h.pause = nil
			h.run(t)
			assert.Equal(t, drained(h.state(t).Drains), []store.DrainKey{{Node: nodeA.ID}},
				"the drain after the undrain must leave the manual drain of the node")
		})

		t.Run("forgets the applied proposals that have expired", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			first := proposalOf(t, 1, proposal.Drain, nodeA.Incarnation, proposal.NodeArgs{Node: nodeA.ID})
			h.submit(t, &first, operator)
			h.clock.Advance(2 * expiry)
			second := proposalOf(t, 2, proposal.Undrain, nodeA.Incarnation, proposal.NodeArgs{Node: nodeA.ID})
			second.Expires = h.clock.Time().Add(expiry)
			h.submit(t, &second, operator)
			applied := slices.Collect(maps.Keys(h.state(t).Applied))
			expect.NotContains(t, applied, first.ID, "the commit must forget the proposal that has expired")
			expect.Contains(t, applied, second.ID, "the commit must record the new proposal")
		})

		t.Run("applies a proposal with the ID of a proposal that it has forgotten", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			first := proposalOf(t, 1, proposal.Drain, nodeA.Incarnation, proposal.NodeArgs{Node: nodeA.ID})
			h.submit(t, &first, operator)
			h.clock.Advance(2 * expiry)
			second := proposalOf(t, 2, proposal.Undrain, nodeA.Incarnation, proposal.NodeArgs{Node: nodeA.ID})
			second.Expires = h.clock.Time().Add(expiry)
			h.submit(t, &second, operator)
			first.Expires = h.clock.Time().Add(expiry)
			got := h.submit(t, &first, operator)
			assert.Nil(t, got.Err, "the controller must apply a proposal whose ID it has forgotten")
		})

		t.Run("returns ErrStale for a drain of a node at another incarnation", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			p := proposalOf(t, 1, proposal.Drain, nodeA.Incarnation+1, proposal.NodeArgs{Node: nodeA.ID})
			got := h.submit(t, &p, operator)
			assert.ErrorIs(t, got.Err, control.ErrStale, "the controller must refuse a drain of another incarnation")
		})

		t.Run("returns ErrStale for a drain of a node that the view does not have", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			p := proposalOf(t, 1, proposal.Drain, nodeC.Incarnation, proposal.NodeArgs{Node: nodeC.ID})
			got := h.submit(t, &p, operator)
			assert.ErrorIs(t, got.Err, control.ErrStale,
				"the controller must refuse a drain of a node that is not live")
		})

		t.Run("returns ErrCall for a drain whose body does not decode", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			p := proposalOf(t, 1, proposal.Drain, nodeA.Incarnation, proposal.NodeArgs{Node: nodeA.ID})
			p.Body = []byte(malformed)
			got := h.submit(t, &p, operator)
			assert.ErrorIs(t, got.Err, control.ErrCall, "the controller must refuse a body that does not decode")
			assert.ErrorIs(t, got.Err, io.ErrUnexpectedEOF, "the error must wrap the error of the decoder")
		})

		t.Run("returns ErrCall for a drain whose body has a key that the arguments do not declare", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			p := proposalOf(t, 1, proposal.Drain, nodeA.Incarnation, map[string]string{unknownKey: string(nodeA.ID)})
			got := h.submit(t, &p, operator)
			assert.ErrorIs(t, got.Err, control.ErrCall,
				"the controller must refuse a key that the arguments do not declare")
		})

		t.Run("moves the writer of a shard to the node of the proposal", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			sid := h.shardOn(t, nodeA.ID)
			p := proposalOf(t, 1, proposal.Move, h.placements(t)[sid].Epoch,
				proposal.MoveArgs{Node: nodeB.ID, Shard: sid})
			got := h.submit(t, &p, operator)
			assert.Nil(t, got.Err, "the controller must apply the move")
			expect.True(t, got.Value.Applied, "the receipt must report that the controller applied the move")
			expect.Equal(t, h.placements(t)[sid].Replicas, []shard.Replica{{Node: nodeB.ID, Role: shard.RolePrimary}},
				"the move must put the writer on the node of the proposal")
			expect.Equal(t, h.metrics.value(movesInstrument, reasonRebalance), 1.0,
				"the controller must count the move")
			expect.Equal(t, h.plans[len(h.plans)-1].Placer.Schedule(), placement.Schedule{},
				"the placer of a move must not schedule another pass")
		})

		t.Run("records a move with the first record of its transition", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			sid := h.shardOn(t, nodeA.ID)
			p := proposalOf(t, 1, proposal.Move, h.placements(t)[sid].Epoch,
				proposal.MoveArgs{Node: nodeB.ID, Shard: sid})
			before := len(h.commits)
			h.submit(t, &p, operator)
			assert.NotEmpty(t, h.commits[before:], "the move must commit")
			first := h.commits[before]
			assert.Length(t, first.Applied, 1, "the first commit of the move must record the proposal")
			expect.Equal(t, first.Applied[0].Proposal.ID, p.ID, "the commit must record the move")
			expect.Equal(t, first.Applied[0].Caller, operator, "the record must contain the caller")
			assert.Length(t, first.Transitions, 1, "the commit must contain the first record of the transition")
			expect.Equal(t, first.Transitions[0].Operation.Shard, sid, "the record must be of the moved shard")
			for _, m := range h.commits[before+1:] {
				expect.Empty(t, m.Applied, "no later commit of the move may record the proposal")
			}
		})

		t.Run("returns ErrCall for a move of a shard that the scope does not have", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			unknown := shard.ID{ID128: id.ID128{0: tagShard}}
			passes := len(h.plans)
			p := proposalOf(t, 1, proposal.Move, epoch.Zero+1, proposal.MoveArgs{Node: nodeB.ID, Shard: unknown})
			got := h.submit(t, &p, operator)
			assert.ErrorIs(t, got.Err, control.ErrCall,
				"the controller must refuse a move of a shard that it does not have")
			assert.Length(t, h.plans, passes, "the controller must refuse the move before a pass")
		})

		t.Run("returns ErrStale for a move whose base is not the epoch of the shard", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			sid := h.shardOn(t, nodeA.ID)
			base := h.placements(t)[sid].Epoch + 1
			passes := len(h.plans)
			p := proposalOf(t, 1, proposal.Move, base, proposal.MoveArgs{Node: nodeB.ID, Shard: sid})
			got := h.submit(t, &p, operator)
			assert.ErrorIs(t, got.Err, control.ErrStale, "the controller must refuse a move of another epoch")
			assert.Length(t, h.plans, passes, "the controller must refuse the move before a pass")
		})

		t.Run("returns ErrBusy for a move of a shard with a transition in flight", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			sid := h.shardOn(t, nodeA.ID)
			h.answer = func(*protocol.Request) error { return protocol.ErrUnreachable }
			h.disrupt(t, disruption(1, nodeA.ID), operator)
			assert.NotEmpty(t, h.state(t).Transitions, "the drain must leave its transitions in flight")
			p := proposalOf(t, 1, proposal.Move, h.placements(t)[sid].Epoch,
				proposal.MoveArgs{Node: nodeB.ID, Shard: sid})
			got := h.submit(t, &p, operator)
			assert.ErrorIs(t, got.Err, control.ErrBusy, "the controller must refuse a move of a shard in transition")
		})

		t.Run("returns ErrStale for a move that the protocol cannot script", func(t *testing.T) {
			t.Parallel()
			h := served(t, selects(config.Handoff, "", func(tb assert.TB, r *config.Registry) error {
				return config.Register(r, config.Handoff, refusing(tb, builtin.BreakBeforeMake()))
			}))
			sid := h.shardOn(t, nodeA.ID)
			p := proposalOf(t, 1, proposal.Move, h.placements(t)[sid].Epoch,
				proposal.MoveArgs{Node: nodeB.ID, Shard: sid})
			got := h.submit(t, &p, operator)
			expect.That(t, got.Err).
				ErrorIs(control.ErrStale, "the controller must refuse a move that the protocol cannot script").
				ErrorIs(handoff.ErrTransfer, "the error must wrap the error of the protocol")
		})

		t.Run("returns ErrCall for a move to the node of the writer", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			sid := h.shardOn(t, nodeA.ID)
			p := proposalOf(t, 1, proposal.Move, h.placements(t)[sid].Epoch,
				proposal.MoveArgs{Node: nodeA.ID, Shard: sid})
			got := h.submit(t, &p, operator)
			assert.ErrorIs(t, got.Err, control.ErrCall, "the controller must refuse a move to the node of the writer")
		})

		t.Run("returns ErrStale for a move to a node that the view does not have", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			sid := h.shardOn(t, nodeA.ID)
			p := proposalOf(t, 1, proposal.Move, h.placements(t)[sid].Epoch,
				proposal.MoveArgs{Node: nodeC.ID, Shard: sid})
			got := h.submit(t, &p, operator)
			assert.ErrorIs(t, got.Err, control.ErrStale, "the controller must refuse a move to a node that is not live")
		})

		t.Run("returns ErrCall for a move whose body does not decode", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			sid := h.shardOn(t, nodeA.ID)
			p := proposalOf(t, 1, proposal.Move, h.placements(t)[sid].Epoch,
				proposal.MoveArgs{Node: nodeB.ID, Shard: sid})
			p.Body = []byte(malformed)
			got := h.submit(t, &p, operator)
			assert.ErrorIs(t, got.Err, control.ErrCall, "the controller must refuse a body that does not decode")
			assert.ErrorIs(t, got.Err, io.ErrUnexpectedEOF, "the error must wrap the error of the decoder")
		})

		t.Run("returns ErrCall for a move that breaks a constraint", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, strict)
			h.start(t)
			h.view(t, nodeA, nodeB, nodeC)
			sid := h.shardOn(t, nodeA.ID)
			p := proposalOf(t, 1, proposal.Move, h.placements(t)[sid].Epoch,
				proposal.MoveArgs{Node: nodeB.ID, Shard: sid})
			got := h.submit(t, &p, operator)
			expect.That(t, got.Err).
				ErrorIs(control.ErrCall, "the controller must refuse a move that breaks a constraint").
				ErrorIs(constraint.ErrMaxPerNode, "the error must wrap the error of the constraint")
			expect.Equal(t, h.placements(t)[sid].Replicas, []shard.Replica{{Node: nodeA.ID, Role: shard.RolePrimary}},
				"the controller must not move a shard for a move that it refuses")
		})

		t.Run("returns ErrBudget for a move of an available writer at maxUnavailable zero", func(t *testing.T) {
			t.Parallel()
			h := served(t, unavailable)
			sid := h.shardOn(t, nodeA.ID)
			p := proposalOf(t, 1, proposal.Move, h.placements(t)[sid].Epoch,
				proposal.MoveArgs{Node: nodeB.ID, Shard: sid})
			got := h.submit(t, &p, operator)
			expect.That(t, got.Err).ErrorIs(control.ErrBudget, "the budget must refuse the move of an available writer")
			expect.Equal(
				t,
				errs.Classify(got.Err),
				errs.Conflict,
				"the refusal of the budget must classify as Conflict",
			)
			expect.Equal(t, h.placements(t)[sid].Replicas, []shard.Replica{{Node: nodeA.ID, Role: shard.RolePrimary}},
				"the controller must not move a shard for a move that the budget refuses")
		})

		t.Run("returns ErrStale for a move to a node that the moves of concurrent passes fill", func(t *testing.T) {
			t.Parallel()
			h := served(t, paired)
			h.view(t, nodeA, nodeB, nodeC)
			h.paused = true
			start := len(h.plans)
			calls := make([]machine.ReplyToken, 0, 3)
			for i, sid := range h.shardIDs(t)[:3] {
				p := proposalOf(t, byte(i+1), proposal.Move, h.placements(t)[sid].Epoch,
					proposal.MoveArgs{Node: nodeC.ID, Shard: sid})
				call := h.submitCall(&p, operator)
				h.step(t, call)
				calls = append(calls, call.Reply)
			}
			for _, plan := range slices.Clone(h.plans[start:]) {
				h.pass(t, plan.Token)
			}
			expect.Nil(t, replyTo[protocol.Receipt](t, h, calls[0]).Err, "the first move must fit node C")
			expect.Nil(t, replyTo[protocol.Receipt](t, h, calls[1]).Err, "the second move must fit node C")
			expect.That(t, replyTo[protocol.Receipt](t, h, calls[2]).Err).
				ErrorIs(control.ErrStale, "the controller must refuse a move that the earlier moves made infeasible").
				ErrorIs(constraint.ErrMaxPerNode, "the error must wrap the error of the constraint")
			expect.Equal(t, h.writers(t)[nodeC.ID], 2, "node C must host two writers at most")
		})

		t.Run("returns ErrCall for a move to a node that the transitions in flight fill", func(t *testing.T) {
			t.Parallel()
			h := served(t, paired)
			h.view(t, nodeA, nodeB, nodeC)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Callback); return ok }
			got := make([]error, 0, 3)
			for i, sid := range h.shardIDs(t)[:3] {
				p := proposalOf(t, byte(i+1), proposal.Move, h.placements(t)[sid].Epoch,
					proposal.MoveArgs{Node: nodeC.ID, Shard: sid})
				got = append(got, h.submit(t, &p, operator).Err)
			}
			expect.Nil(t, got[0], "the first move must fit node C")
			expect.Nil(t, got[1], "the second move must fit node C")
			expect.That(t, got[2]).
				ErrorIs(control.ErrCall, "the pass of the third move must count the transitions to node C").
				ErrorIs(constraint.ErrMaxPerNode, "the error must wrap the error of the constraint")
			expect.Length(t, h.state(t).Transitions, 2, "the third move must not start a transition")
		})

		t.Run("returns ErrCall for a move whose admission does not build", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			sid := h.shardOn(t, nodeA.ID)
			h.paused = true
			p := proposalOf(t, 1, proposal.Move, h.placements(t)[sid].Epoch,
				proposal.MoveArgs{Node: nodeB.ID, Shard: sid})
			call := h.submitCall(&p, operator)
			h.step(t, call)
			pass := h.plans[len(h.plans)-1].Token
			h.view(t, nodeA, nodeB, nameless)
			h.pass(t, pass)
			expect.That(t, replyTo[protocol.Receipt](t, h, call.Reply).Err).
				ErrorIs(control.ErrCall, "the controller must refuse a move whose admission does not build").
				ErrorIs(placement.ErrInput, "the error must wrap the error of the state")
		})

		t.Run("takes a proposal with the ID of a move that it refused", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, strict)
			h.start(t)
			h.view(t, nodeA, nodeB, nodeC)
			sid := h.shardOn(t, nodeA.ID)
			p := proposalOf(t, 1, proposal.Move, h.placements(t)[sid].Epoch,
				proposal.MoveArgs{Node: nodeB.ID, Shard: sid})
			h.submit(t, &p, operator)
			got := h.submit(t, &p, operator)
			assert.ErrorIs(t, got.Err, control.ErrCall, "the controller must check a refused proposal again")
		})

		t.Run("returns ErrApplied for a proposal whose pass is in flight", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			sid := h.shardOn(t, nodeA.ID)
			h.paused = true
			p := proposalOf(t, 1, proposal.Move, h.placements(t)[sid].Epoch,
				proposal.MoveArgs{Node: nodeB.ID, Shard: sid})
			h.step(t, h.submitCall(&p, operator))
			call := h.submitCall(&p, operator)
			h.step(t, call)
			assert.ErrorIs(t, replyTo[protocol.Receipt](t, h, call.Reply).Err, control.ErrApplied,
				"the controller must refuse a proposal whose pass is in flight")
		})

		t.Run("returns ErrStale for a move of a shard that another move moved during its pass", func(t *testing.T) {
			t.Parallel()
			h := served(t, quiet)
			h.view(t, nodeA, nodeB, nodeC)
			sid := h.shardOn(t, nodeA.ID)
			base := h.placements(t)[sid].Epoch
			h.paused = true
			toC := proposalOf(t, 1, proposal.Move, base, proposal.MoveArgs{Node: nodeC.ID, Shard: sid})
			h.step(t, h.submitCall(&toC, operator))
			first := h.plans[len(h.plans)-1].Token
			toB := proposalOf(t, 2, proposal.Move, base, proposal.MoveArgs{Node: nodeB.ID, Shard: sid})
			call := h.submitCall(&toB, operator)
			h.step(t, call)
			second := h.plans[len(h.plans)-1].Token
			h.pass(t, first)
			h.pass(t, second)
			assert.ErrorIs(t, replyTo[protocol.Receipt](t, h, call.Reply).Err, control.ErrStale,
				"the controller must refuse a move whose shard another move moved")
			expect.Equal(t, h.placements(t)[sid].Replicas, []shard.Replica{{Node: nodeC.ID, Role: shard.RolePrimary}},
				"the shard must be on the node of the first move")
		})

		t.Run("returns ErrBusy for a move that the bounds refuse", func(t *testing.T) {
			t.Parallel()
			h := served(t, func(s *setup) { s.spec.Budget.MaxTransitions = new(1) })
			h.answer = func(*protocol.Request) error { return protocol.ErrUnreachable }
			moving := h.shardOn(t, nodeA.ID)
			first := proposalOf(t, 1, proposal.Move, h.placements(t)[moving].Epoch,
				proposal.MoveArgs{Node: nodeB.ID, Shard: moving})
			h.submit(t, &first, operator)
			sid := h.shardOn(t, nodeB.ID)
			p := proposalOf(t, 2, proposal.Move, h.placements(t)[sid].Epoch,
				proposal.MoveArgs{Node: nodeA.ID, Shard: sid})
			got := h.submit(t, &p, operator)
			assert.ErrorIs(t, got.Err, control.ErrBusy, "the controller must refuse a move above the bounds")
		})

		t.Run("returns ErrStale for a move of a shard that moved during its pass", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			sid := h.shardOn(t, nodeA.ID)
			h.paused = true
			p := proposalOf(t, 1, proposal.Move, h.placements(t)[sid].Epoch,
				proposal.MoveArgs{Node: nodeB.ID, Shard: sid})
			call := h.submitCall(&p, operator)
			h.step(t, call)
			pass := h.plans[len(h.plans)-1].Token
			h.disrupt(t, disruption(1, nodeA.ID), operator)
			h.pass(t, h.plans[len(h.plans)-1].Token)
			h.paused = false
			h.pass(t, pass)
			assert.ErrorIs(t, replyTo[protocol.Receipt](t, h, call.Reply).Err, control.ErrStale,
				"the controller must refuse a move whose shard moved during its pass")
		})

		t.Run("returns ErrBusy for a move of a shard whose transition started during its pass", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			sid := h.shardOn(t, nodeA.ID)
			h.paused = true
			p := proposalOf(t, 1, proposal.Move, h.placements(t)[sid].Epoch,
				proposal.MoveArgs{Node: nodeB.ID, Shard: sid})
			call := h.submitCall(&p, operator)
			h.step(t, call)
			pass := h.plans[len(h.plans)-1].Token
			h.answer = func(*protocol.Request) error { return protocol.ErrUnreachable }
			h.disrupt(t, disruption(1, nodeA.ID), operator)
			h.pass(t, h.plans[len(h.plans)-1].Token)
			h.pass(t, pass)
			assert.ErrorIs(t, replyTo[protocol.Receipt](t, h, call.Reply).Err, control.ErrBusy,
				"the controller must refuse a move whose shard has a transition for now")
		})

		t.Run("returns ErrStale for a move to a node that started to drain during its pass", func(t *testing.T) {
			t.Parallel()
			h := spare(t)
			sid := h.shardOn(t, nodeA.ID)
			h.paused = true
			p := proposalOf(t, 1, proposal.Move, h.placements(t)[sid].Epoch,
				proposal.MoveArgs{Node: nodeC.ID, Shard: sid})
			call := h.submitCall(&p, operator)
			h.step(t, call)
			pass := h.plans[len(h.plans)-1].Token
			h.disrupt(t, disruption(1, nodeC.ID), operator)
			h.pass(t, pass)
			assert.ErrorIs(t, replyTo[protocol.Receipt](t, h, call.Reply).Err, control.ErrStale,
				"the controller must refuse a move to a node that started to drain during its pass")
		})

		t.Run("explains a shard with the cost of each tier of its goals", func(t *testing.T) {
			t.Parallel()
			h := served(t, func(s *setup) {
				s.spec.Placement = config.Selection{Name: planner.Name, Config: json.RawMessage(tieredConfig)}
			})
			h.view(t, nodeA, nodeB, nodeC, nodeD)
			gen := h.state(t).Head.Generation
			p := proposalOf(t, 1, proposal.Explain, gen, proposal.ShardArgs{Shard: h.shardOn(t, nodeA.ID)})
			got := h.submit(t, &p, operator)
			assert.Nil(t, got.Err, "the controller must explain the shard")
			st, err := placement.NewState(h.plans[len(h.plans)-1].Input)
			assert.NoError(t, err, "the input of the explanation must build a state")
			count, err := goal.NewCount(goal.CountConfig{})
			assert.NoError(t, err, "the goal count must build")
			v := count.Cost(st)
			assert.NotEqual(t, v, 0.0, "the nodes of the test must have unequal counts")
			exp := proposal.Explanation{Violations: []proposal.Violation{}, Costs: []float64{v + 3*v, 2 * v}}
			want, err := json.Marshal(&exp)
			assert.NoError(t, err, "the explanation must encode")
			expect.Equal(t, string(got.Value.Result), string(want), "the result must sum the costs of each tier")
			expect.False(t, got.Value.Applied, "the controller must not commit an explanation")
			expect.Equal(t, got.Value.Generation, gen, "the receipt must contain the generation of the explanation")
			expect.Equal(t, got.Value.Incarnation, incA, "the receipt must contain the namespace incarnation")
			expect.Equal(t, h.state(t).Head.Generation, gen, "an explanation must not commit")
			expect.Equal(t, h.plans[len(h.plans)-1].Placer.Schedule(), placement.Schedule{},
				"the placer of an explanation must not schedule another pass")
		})

		t.Run("explains a shard with the violations of its replicas", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, strict, crowded(t, nodeA))
			h.start(t)
			h.view(t, nodeA)
			p := proposalOf(t, 1, proposal.Explain, h.state(t).Head.Generation,
				proposal.ShardArgs{Shard: h.shardOn(t, nodeA.ID)})
			got := h.submit(t, &p, operator)
			var exp proposal.Explanation
			assert.NoError(t, json.Unmarshal(got.Value.Result, &exp), "the result must be an explanation")
			assert.Length(t, exp.Violations, 1, "the explanation must contain the violations of its shard alone")
			v := exp.Violations[0]
			expect.Equal(t, v.Constraint, constraint.MaxPerNodeName, "the violation must name the constraint")
			expect.Equal(t, v.Node, nodeA.ID, "the violation must name the node")
			expect.Equal(t, v.Role, shard.RolePrimary, "the violation must contain the role of the replica")
			expect.Equal(t, v.Reason, constraint.ErrMaxPerNode.Error(), "the violation must contain the error")
		})

		costs := []struct {
			name string
			goal shard.StrategyName
			cost float64
		}{
			{name: "returns ErrCall for an explanation whose cost is not a number", goal: nanName, cost: math.NaN()},
			{name: "returns ErrCall for an explanation whose cost is infinite", goal: infName, cost: math.Inf(1)},
		}
		for _, tt := range costs {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				raw := fmt.Sprintf(`{"goals":[{"name":%q,"tier":0,"weight":1}]}`, tt.goal)
				h := newHarness(t, crowded(t, nodeA), func(s *setup) {
					s.extend = func(tb assert.TB, r *config.Registry) error {
						return config.Register(r, config.Goal, badCost(tb, tt.goal, tt.cost))
					}
					s.spec.Placement = config.Selection{Name: planner.Name, Config: json.RawMessage(raw)}
				})
				h.start(t)
				h.view(t, nodeA)
				p := proposalOf(t, 1, proposal.Explain, h.state(t).Head.Generation,
					proposal.ShardArgs{Shard: h.shardOn(t, nodeA.ID)})
				got := h.submit(t, &p, operator)
				expect.That(t, got.Err).
					ErrorIs(control.ErrCall, "the controller must refuse an explanation that fails").
					ErrorIs(placement.ErrCost, "the error must wrap the error of the cost")
			})
		}

		t.Run("returns ErrCall for an explanation of a state that does not build", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.view(t, nodeA, nodeB, nameless)
			p := proposalOf(t, 1, proposal.Explain, h.state(t).Head.Generation,
				proposal.ShardArgs{Shard: h.shardOn(t, nodeA.ID)})
			got := h.submit(t, &p, operator)
			expect.That(t, got.Err).
				ErrorIs(control.ErrCall, "the controller must refuse an explanation that fails").
				ErrorIs(placement.ErrInput, "the error must wrap the error of the state")
		})

		t.Run("returns placement.ErrInput from the placer of an explanation of a state that does not build",
			func(t *testing.T) {
				t.Parallel()
				h := served(t)
				h.view(t, nodeA, nodeB, nameless)
				p := proposalOf(t, 1, proposal.Explain, h.state(t).Head.Generation,
					proposal.ShardArgs{Shard: h.shardOn(t, nodeA.ID)})
				h.submit(t, &p, operator)
				pass := h.plans[len(h.plans)-1]
				_, err := pass.Placer.Place(t.Context(), pass.Input)
				assert.ErrorIs(t, err, placement.ErrInput, "the placer must refuse a state that does not build")
			})

		t.Run("explains a shard again for the ID of an earlier explanation", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			p := proposalOf(t, 1, proposal.Explain, h.state(t).Head.Generation,
				proposal.ShardArgs{Shard: h.shardOn(t, nodeA.ID)})
			h.submit(t, &p, operator)
			got := h.submit(t, &p, operator)
			assert.Nil(t, got.Err, "the controller must explain a shard again, because it records no explanation")
		})

		t.Run("returns ErrCall for a kind that the config does not list", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			p := proposalOf(t, 1, proposal.SpecChange, h.state(t).Head.Generation,
				proposal.ShardArgs{Shard: h.shardOn(t, nodeA.ID)})
			got := h.submit(t, &p, operator)
			assert.ErrorIs(t, got.Err, control.ErrCall, "the controller must refuse a kind that it does not apply")
		})

		t.Run("returns ErrStale for an explanation of another generation", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			p := proposalOf(t, 1, proposal.Explain, h.state(t).Head.Generation+1,
				proposal.ShardArgs{Shard: h.shardOn(t, nodeA.ID)})
			got := h.submit(t, &p, operator)
			assert.ErrorIs(t, got.Err, control.ErrStale,
				"the controller must refuse an explanation of another generation")
		})

		t.Run("returns ErrCall for an explanation of a shard that the scope does not have", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			unknown := shard.ID{ID128: id.ID128{0: tagShard}}
			p := proposalOf(t, 1, proposal.Explain, h.state(t).Head.Generation, proposal.ShardArgs{Shard: unknown})
			got := h.submit(t, &p, operator)
			assert.ErrorIs(t, got.Err, control.ErrCall,
				"the controller must refuse an explanation of a shard that it does not have")
		})

		t.Run("returns ErrCall for an explanation whose body does not decode", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			p := proposalOf(t, 1, proposal.Explain, h.state(t).Head.Generation,
				proposal.ShardArgs{Shard: h.shardOn(t, nodeA.ID)})
			p.Body = []byte(malformed)
			got := h.submit(t, &p, operator)
			assert.ErrorIs(t, got.Err, control.ErrCall, "the controller must refuse a body that does not decode")
			assert.ErrorIs(t, got.Err, io.ErrUnexpectedEOF, "the error must wrap the error of the decoder")
		})

		t.Run("applies a proposal of any caller without authentication", func(t *testing.T) {
			t.Parallel()
			h := served(t, func(s *setup) { s.cfg.Unauthenticated = true })
			p := proposalOf(t, 1, proposal.Drain, nodeA.Incarnation, proposal.NodeArgs{Node: nodeA.ID})
			got := h.submit(t, &p, unknownCaller)
			assert.Nil(t, got.Err, "the controller must apply a proposal of any caller without authentication")
		})

		tests := []struct {
			want   error
			give   func(*proposal.Proposal)
			name   string
			caller shard.Identity
		}{
			{
				name:   "returns ErrDenied for a caller that is not an operator",
				give:   func(*proposal.Proposal) {},
				caller: unknownCaller,
				want:   control.ErrDenied,
			},
			{
				name:   "returns ErrCall for a proposal that has expired",
				give:   func(p *proposal.Proposal) { p.Expires = origin.Add(-time.Second) },
				caller: operator,
				want:   control.ErrCall,
			},
			{
				name:   "returns ErrCall for a proposal of another scope",
				give:   func(p *proposal.Proposal) { p.Scope.Cell = otherCell },
				caller: operator,
				want:   control.ErrCall,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				h := served(t)
				p := proposalOf(t, 1, proposal.Drain, nodeA.Incarnation, proposal.NodeArgs{Node: nodeA.ID})
				tt.give(&p)
				got := h.submit(t, &p, tt.caller)
				assert.ErrorIs(t, got.Err, tt.want, "the controller must refuse the proposal")
				expect.Empty(t, h.state(t).Drains, "a refused proposal must not change the scope")
			})
		}

		t.Run("returns ErrApplied for a proposal that the scope has applied", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			p := proposalOf(t, 1, proposal.Drain, nodeA.Incarnation, proposal.NodeArgs{Node: nodeA.ID})
			h.submit(t, &p, operator)
			got := h.submit(t, &p, operator)
			assert.ErrorIs(t, got.Err, control.ErrApplied, "the controller must refuse a proposal that it has applied")
		})

		t.Run("returns ErrApplied for a proposal that its scope applied before the acquisition", func(t *testing.T) {
			t.Parallel()
			p := proposalOf(t, 1, proposal.Drain, nodeA.Incarnation, proposal.NodeArgs{Node: nodeA.ID})
			applied := store.Applied{Caller: operator, Proposal: p}
			h := served(t, func(s *setup) { s.prior = []*store.Mutation{{Applied: []store.Applied{applied}}} })
			got := h.submit(t, &p, operator)
			assert.ErrorIs(t, got.Err, control.ErrApplied,
				"the controller must refuse a proposal that its scope has applied")
		})

		t.Run("returns ErrApplied for a proposal that a commit in flight applies", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			p := proposalOf(t, 1, proposal.Drain, nodeA.Incarnation, proposal.NodeArgs{Node: nodeA.ID})
			h.step(t, h.submitCall(&p, operator))
			call := h.submitCall(&p, operator)
			h.step(t, call)
			assert.ErrorIs(t, replyTo[protocol.Receipt](t, h, call.Reply).Err, control.ErrApplied,
				"the controller must refuse a proposal that a commit in flight applies")
		})

		t.Run("returns ErrNotServing before the controller serves", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			p := proposalOf(t, 1, proposal.Drain, nodeA.Incarnation, proposal.NodeArgs{Node: nodeA.ID})
			h.step(t, h.submitCall(&p, operator))
			assert.ErrorIs(t, last[machine.Reply[protocol.Receipt]](t, h).Err, control.ErrNotServing,
				"the controller must refuse a proposal before it serves")
		})

		t.Run("replies with ErrNotServing to a proposal that waits when the controller stops", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			p := proposalOf(t, 1, proposal.Drain, nodeA.Incarnation, proposal.NodeArgs{Node: nodeA.ID})
			call := h.submitCall(&p, operator)
			h.step(t, call)
			h.stop(t)
			assert.ErrorIs(t, replyTo[protocol.Receipt](t, h, call.Reply).Err, control.ErrNotServing,
				"a proposal that waits for its commit must get ErrNotServing when the controller stops")
		})

		t.Run("applies a spec with one commit that records the proposal", func(t *testing.T) {
			t.Parallel()
			h := served(t, rooted(t))
			p := proposalOf(t, 1, proposal.SpecChange, firstVersion, testSpec(firstVersion+1))
			got := h.submit(t, &p, operator)
			assert.Nil(t, got.Err, "the controller must apply the spec")
			expect.True(t, got.Value.Applied, "the receipt must report that the controller applied the spec")
			expect.Equal(t, got.Value.Generation, h.state(t).Head.Generation,
				"the receipt must contain the generation of the commit")
			st := h.state(t)
			var accepted config.Spec
			assert.NoError(t, json.Unmarshal(st.Spec, &accepted), "the scope must contain a spec")
			expect.Equal(t, accepted.Version, firstVersion+1, "the scope must accept the spec of the proposal")
			expect.Contains(t, slices.Collect(maps.Keys(st.Applied)), p.ID, "the scope must record the proposal")
		})

		t.Run("commits the spec with every default", func(t *testing.T) {
			t.Parallel()
			h := served(t, rooted(t))
			p := proposalOf(t, 1, proposal.SpecChange, firstVersion, testSpec(firstVersion+1))
			h.submit(t, &p, operator)
			want := testSpec(firstVersion + 1)
			res, err := config.Resolve(h.registry, &want, provided)
			assert.NoError(t, err, "the spec of the proposal must resolve")
			raw, err := json.Marshal(&res.Spec)
			assert.NoError(t, err, "the resolved spec must encode")
			assert.Equal(t, string(h.state(t).Spec), string(raw), "the scope must contain the resolved spec")
		})

		specs := []struct {
			want  error
			give  func(*proposal.Proposal)
			name  string
			setup option
		}{
			{
				name: "returns ErrStale for a spec whose base is not the accepted version",
				give: func(p *proposal.Proposal) { p.Base++ },
				want: control.ErrStale,
			},
			{
				name: "returns ErrCall for a spec whose version does not follow the base",
				give: func(p *proposal.Proposal) { p.Body = specBody(t, testSpec(firstVersion+2)) },
				want: control.ErrCall,
			},
			{
				name: "returns ErrCall for a spec whose body does not decode",
				give: func(p *proposal.Proposal) { p.Body = []byte(malformed) },
				want: control.ErrCall,
			},
			{
				name: "returns the error of the decoder for a spec whose body does not decode",
				give: func(p *proposal.Proposal) { p.Body = []byte(malformed) },
				want: io.ErrUnexpectedEOF,
			},
			{
				name: "returns ErrSpec for a spec that does not resolve",
				give: func(p *proposal.Proposal) {
					s := testSpec(firstVersion + 1)
					s.Preset = "unknown"
					p.Body = specBody(t, s)
				},
				want: control.ErrSpec,
			},
			{
				name: "returns ErrSpec for a spec whose strategies do not build",
				give: func(p *proposal.Proposal) {
					s := testSpec(firstVersion + 1)
					s.Placement = config.Selection{Name: planner.Name, Config: json.RawMessage(fragileConstraint)}
					p.Body = specBody(t, s)
				},
				setup: func(s *setup) {
					s.extend = func(tb assert.TB, r *config.Registry) error {
						return config.Register(r, config.Constraint, fragile(tb, builtin.MaxPerNode()))
					}
				},
				want: control.ErrSpec,
			},
			{
				name: "returns ErrSpec for a change that the namespace cannot take",
				give: func(p *proposal.Proposal) {
					s := testSpec(firstVersion + 1)
					s.Partitioning = config.Selection{Name: fixedslots.Name, Config: json.RawMessage(otherSlots)}
					p.Body = specBody(t, s)
				},
				want: config.ErrChange,
			},
			{
				name:  "returns ErrSpec for an accepted spec that does not resolve",
				give:  func(*proposal.Proposal) {},
				setup: func(s *setup) { s.accepted = []byte(malformed) },
				want:  control.ErrSpec,
			},
		}
		for _, tt := range specs {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				opts := []option{rooted(t)}
				if tt.setup != nil {
					opts = append(opts, tt.setup)
				}
				h := served(t, opts...)
				before := h.state(t).Spec
				p := proposalOf(t, 1, proposal.SpecChange, firstVersion, testSpec(firstVersion+1))
				tt.give(&p)
				got := h.submit(t, &p, operator)
				expect.ErrorIs(t, got.Err, tt.want, "the controller must refuse the spec")
				expect.Equal(t, h.state(t).Spec, before, "a refused spec must not change the scope")
			})
		}

		t.Run("returns ErrStale for a second spec of the base of a spec whose commit is in flight",
			func(t *testing.T) {
				t.Parallel()
				h := served(t, rooted(t))
				first := proposalOf(t, 1, proposal.SpecChange, firstVersion, testSpec(firstVersion+1))
				h.step(t, h.submitCall(&first, operator))
				second := proposalOf(t, 2, proposal.SpecChange, firstVersion, testSpec(firstVersion+1))
				call := h.submitCall(&second, operator)
				h.step(t, call)
				assert.ErrorIs(t, replyTo[protocol.Receipt](t, h, call.Reply).Err, control.ErrStale,
					"the controller must check the base of a spec against the spec of the commit in flight")
			})
	})
}

// rooted returns an option whose controller applies spec proposals alone, as the root of a
// namespace does. Its acquisition commits the accepted spec of the setup, which is the
// resolved spec of the tests unless a later option changes it.
func rooted(tb assert.TB) option {
	tb.Helper()
	spec := testSpec(firstVersion)
	res, err := config.Resolve(registry(tb), &spec, provided)
	assert.NoError(tb, err, "the spec of the tests must resolve")
	raw, err := json.Marshal(&res.Spec)
	assert.NoError(tb, err, "the resolved spec must encode")
	return func(s *setup) {
		s.accepted = raw
		s.cfg.Kinds = []proposal.OperationKind{proposal.SpecChange}
		s.cfg.Acquisition = func(_ store.Head, m *store.Mutation) shard.NamespaceIncarnation {
			m.Spec = s.accepted
			return incA
		}
	}
}

// specBody returns the body of a spec proposal of the spec s.
func specBody(tb assert.TB, s config.Spec) []byte {
	tb.Helper()
	body, err := json.Marshal(&s)
	assert.NoError(tb, err, "the spec must encode")
	return body
}

// proposalOf returns a proposal of the scope of the cell with the ID n. The proposal has
// the kind of change k, the base b and the body args, and expires an hour after the start
// of the clock.
func proposalOf(tb assert.TB, n byte, k proposal.OperationKind, b epoch.Epoch, args any) proposal.Proposal {
	tb.Helper()
	body, err := json.Marshal(args)
	assert.NoError(tb, err, "the arguments of the proposal must encode")
	return proposal.Proposal{
		Scope:       cellScope,
		Expires:     origin.Add(expiry),
		Body:        body,
		Base:        b,
		Incarnation: incA,
		ID:          proposal.ID{ID: id.New128([16]byte{0: tagProposal, 15: n})},
		Kind:        k,
	}
}

// submitCall returns the Submit of p from caller as the next inbound call.
func (h *harness) submitCall(p *proposal.Proposal, caller shard.Identity) machine.Submit {
	return machine.Submit{
		Proposal: p,
		Metadata: protocol.Metadata{Caller: caller, Version: protocol.Current},
		Reply:    h.call(),
	}
}

// submit passes the Submit of p from caller and runs the machine. It returns the reply to
// the call.
func (h *harness) submit(tb assert.TB, p *proposal.Proposal, caller shard.Identity) machine.Reply[protocol.Receipt] {
	tb.Helper()
	call := h.submitCall(p, caller)
	h.step(tb, call)
	h.run(tb)
	return replyTo[protocol.Receipt](tb, h, call.Reply)
}
