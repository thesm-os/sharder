// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent_test

import (
	"slices"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/agent"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// thirdNode is a node of the cell other than the node of the tests and otherNode.
const thirdNode shard.NodeID = "node-c"

// TestWatch checks the state of the scope of its cell that an agent keeps.
func TestWatch(t *testing.T) {
	t.Parallel()

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("stops when the root creates the namespace again", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			i := slices.IndexFunc(h.watches, func(w machine.Watch) bool { return w.Scope == cellScope })
			assert.True(t, i >= 0, "the agent must watch the scope of its cell")
			h.step(t, machine.Watched{Err: shard.ErrIncarnation, Token: h.watches[i].Token})
			assert.ErrorIs(t, h.machine.Err(), shard.ErrIncarnation,
				"the agent must stop when the namespace has another incarnation")
		})

		t.Run("marks an operation ended when a snapshot does not have its transition", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.place(t, shardA, firstEpoch)
			op := shard.OperationID{Incarnation: incA, Shard: shardA, Sequence: 1}
			h.commitCell(t, &store.Mutation{Transitions: []store.Transition{transition(op, shard.PhaseRunning)}})
			h.follow(t)
			h.commitCell(t, &store.Mutation{Done: []shard.ID{shardA}})
			h.compact(t)
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch)))
			assert.ErrorIs(t, got.Err, agent.ErrStale, "the snapshot must end the operation that it does not have")
		})

		t.Run("keeps an operation whose transition a snapshot still has", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.place(t, shardA, firstEpoch)
			op := shard.OperationID{Incarnation: incA, Shard: shardA, Sequence: 1}
			h.commitCell(t, &store.Mutation{Transitions: []store.Transition{transition(op, shard.PhaseRunning)}})
			h.follow(t)
			h.compact(t)
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch)))
			assert.NoError(t, got.Err, "the snapshot must keep the operation that it has")
		})

		t.Run("keeps the operation of a callback that arrives after the read of a snapshot", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.pause = func(e machine.Effect) bool { r, ok := e.(machine.Read); return ok && r.Scope == cellScope }
			h.compact(t)
			i := slices.IndexFunc(h.pending, func(e machine.Effect) bool { _, ok := e.(machine.Read); return ok })
			assert.True(t, i >= 0, "the agent must read a snapshot after the compaction")
			read := h.perform(t, h.pending[i])
			h.pending = slices.Delete(h.pending, i, i+1)
			h.pause = nil
			op := shard.OperationID{Incarnation: incA, Shard: shardA, Sequence: 1}
			h.commitCell(t, &store.Mutation{
				Put:         []shard.Placement{placement(shardA, secondEpoch, node.ID)},
				Transitions: []store.Transition{transition(op, shard.PhaseRunning)},
			})
			to := h.do(t, h.request(protocol.Add, shardA, 1, secondEpoch))
			h.step(t, read)
			h.run(t)
			h.follow(t)
			expect.NoError(t, replyTo[struct{}](t, h, to).Err, "the add must succeed once its placement arrives")
			again := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Add, shardA, 1, secondEpoch)))
			expect.NoError(t, again.Err, "a snapshot that the agent read before a callback must not end its operation")
		})

		t.Run("keeps a newer operation when the watch ends an older one", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.place(t, shardA, firstEpoch)
			older := shard.OperationID{Incarnation: incA, Shard: shardA, Sequence: 1}
			h.commitCell(t, &store.Mutation{Transitions: []store.Transition{transition(older, shard.PhaseRunning)}})
			h.commitCell(t, &store.Mutation{Done: []shard.ID{shardA}})
			newer := shard.OperationID{Incarnation: incA, Shard: shardA, Sequence: 2}
			h.commitCell(t, &store.Mutation{
				Put:         []shard.Placement{placement(shardA, secondEpoch, node.ID)},
				Transitions: []store.Transition{transition(newer, shard.PhaseRunning)},
			})
			to := h.do(t, h.request(protocol.Add, shardA, 2, secondEpoch))
			h.follow(t)
			expect.NoError(t, replyTo[struct{}](t, h, to).Err, "the add of the newer operation must succeed")
			again := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Add, shardA, 2, secondEpoch)))
			expect.NoError(t, again.Err, "the end of the older operation must not end the newer one")
		})

		t.Run("learns the abort path of an operation from the watch", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.place(t, shardA, firstEpoch)
			op := shard.OperationID{Incarnation: incA, Shard: shardA, Sequence: 1}
			h.commitCell(t, &store.Mutation{Transitions: []store.Transition{transition(op, shard.PhaseAborting)}})
			h.follow(t)
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch)))
			assert.ErrorIs(t, got.Err, agent.ErrStale, "a forward callback of an aborting operation must fail")
		})

		t.Run("ignores the abort path of an operation older than the newest", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.place(t, shardA, firstEpoch)
			h.do(t, h.request(protocol.Add, shardA, 2, firstEpoch))
			op := shard.OperationID{Incarnation: incA, Shard: shardA, Sequence: 1}
			h.commitCell(t, &store.Mutation{Transitions: []store.Transition{transition(op, shard.PhaseAborting)}})
			h.follow(t)
			forward := h.request(protocol.Drop, shardA, 2, firstEpoch)
			forward.Step = 1
			got := replyTo[struct{}](t, h, h.do(t, forward))
			assert.NoError(t, got.Err, "the abort path of an older operation must not refuse the newest")
		})

		t.Run("ignores a transition without an endpoint on the node", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.place(t, shardA, firstEpoch)
			op := shard.OperationID{Incarnation: incA, Shard: shardA, Sequence: 1}
			other := transition(op, shard.PhaseAborting)
			other.To.Node = thirdNode
			h.commitCell(t, &store.Mutation{Transitions: []store.Transition{other}})
			h.compact(t)
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch)))
			assert.NoError(t, got.Err, "the agent must not learn from a transition of other nodes")
		})

		t.Run("tracks a transition whose source is the node", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			op := shard.OperationID{Incarnation: incA, Shard: shardA, Sequence: 2}
			from := transition(op, shard.PhaseAborting)
			from.From.Node, from.To.Node = node.ID, otherNode
			h.commitCell(t, &store.Mutation{Transitions: []store.Transition{from}})
			h.follow(t)
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Drop, shardA, 2, firstEpoch)))
			assert.ErrorIs(t, got.Err, agent.ErrStale, "the agent must learn from a transition from its node")
		})

		t.Run("tracks a transition whose fallback is the node", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			op := shard.OperationID{Incarnation: incA, Shard: shardA, Sequence: 2}
			fallback := transition(op, shard.PhaseAborting)
			fallback.To.Node, fallback.Fallback.Node = thirdNode, node.ID
			h.commitCell(t, &store.Mutation{Transitions: []store.Transition{fallback}})
			h.compact(t)
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Drop, shardA, 2, firstEpoch)))
			assert.ErrorIs(
				t,
				got.Err,
				agent.ErrStale,
				"the agent must learn from a transition with its node as fallback",
			)
		})

		t.Run("ends no operation for a transition that ends on another node", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.place(t, shardA, firstEpoch)
			h.commitCell(t, &store.Mutation{Done: []shard.ID{shardB}})
			h.follow(t)
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch)))
			assert.NoError(t, got.Err, "the end of a transition that the agent did not track must change nothing")
		})

		t.Run("reports to the controller of a newer record", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			before := len(h.reports)
			h.commitCell(t, &store.Mutation{Cell: record(cellTerm + 1)})
			h.follow(t)
			expect.Length(t, h.reports, before+1, "a newer record must start a report")
		})

		t.Run("returns ErrStale for a controller epoch below the epoch of a newer record", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.commitCell(t, &store.Mutation{Cell: record(cellTerm + 1)})
			h.follow(t)
			h.place(t, shardA, firstEpoch)
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch)))
			assert.ErrorIs(
				t,
				got.Err,
				agent.ErrStale,
				"a newer record must refuse the epoch of the controller before it",
			)
		})

		t.Run("starts an add that waited for its placement once", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			h.place(t, shardA, firstEpoch)
			h.commitCell(t, &store.Mutation{})
			h.follow(t)
			assert.Length(t, h.acquires, 1, "a later generation must not start the add again")
		})

		t.Run("does not report again for a record of the same controller", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			before := len(h.reports)
			c := record(cellTerm)
			c.SpecVersion++
			h.commitCell(t, &store.Mutation{Cell: c})
			h.follow(t)
			expect.Length(t, h.reports, before, "a record of the same controller must not start a report")
		})

		t.Run("takes the placements of a snapshot of more than one page", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			ids := []shard.ID{shardA, shardB, shardC}
			ps := make([]shard.Placement, 0, len(ids))
			for _, sid := range ids {
				ps = append(ps, placement(sid, firstEpoch, node.ID))
			}
			h.commitCell(t, &store.Mutation{Put: ps})
			h.start(t)
			for i, sid := range ids {
				h.do(t, h.request(protocol.Add, sid, uint64(i)+1, firstEpoch))
			}
			assert.Length(t, h.acquires, len(ids), "the snapshot must list each placement of each page")
		})

		t.Run("keeps the tenure of a placement on a later page of a snapshot", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			ids := []shard.ID{shardA, shardB, shardC}
			for i, sid := range ids {
				h.serve(t, sid, uint64(i)+1, firstEpoch)
			}
			h.compact(t)
			assert.Length(t, h.handles, len(ids), "a snapshot must not clean up a tenure that a later page lists")
		})
	})
}
