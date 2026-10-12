// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package engine_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// TestTransition checks the requests that the steps of a transition send, and the
// placements that its commit steps commit.
func TestTransition(t *testing.T) {
	t.Parallel()

	t.Run("Applied", func(t *testing.T) {
		t.Parallel()

		t.Run("sends a drop to the source at the epoch of the shard", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.flush(t)
			assert.Length(t, h.calls, 1, "the first batch must send the drop")
			assert.Equal(t, *h.calls[0].Request, protocol.Request{
				Scope:           cellScope,
				Node:            nodeA.ID,
				Range:           p.Range,
				Operation:       shard.OperationID{Incarnation: incA, Shard: p.Shard, Sequence: uint64(placed) + 1},
				Step:            0,
				NodeIncarnation: nodeA.Incarnation,
				Epoch:           placed,
				ControllerEpoch: term,
				Shard:           p.Shard,
				Call:            protocol.Drop,
				Path:            protocol.Forward,
				Role:            shard.RolePrimary,
			}, "the drop must address the source at the epoch of the shard")
		})

		t.Run("sends the add to the destination at the new epoch", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.flush(t)
			h.answerOne(t)
			h.flush(t)
			assert.Length(t, h.calls, 1, "the second batch must send the add")
			r := h.calls[0].Request
			expect.Equal(t, r.Node, nodeB.ID, "the add must address the destination")
			expect.Equal(t, r.Epoch, newEpoch, "the add must have the new epoch")
			expect.Equal(t, r.Step, 2, "the add must be the third step")
			expect.Equal(t, r.Call, protocol.Add, "the third step must be an add")
			expect.Equal(t, h.calls[0].Timeout, addTimeout, "the callback must have the timeout of its step")
		})

		t.Run("sends the add of the abort path to the fallback at the abort epoch", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB, nodeC)
			h.answer = refuse(protocol.Add, nodeB.ID)
			h.start(t, moveOf(p, nodeA, nodeB, nodeC))
			h.drain(t)
			assert.Length(t, h.sent, 4, "the transition must send four callbacks")
			r := h.sent[2]
			expect.Equal(t, r.Node, nodeC.ID, "the add of the abort path must address the fallback")
			expect.Equal(t, r.Epoch, abortEpoch, "the add of the abort path must have the abort epoch")
			expect.Equal(t, r.Path, protocol.Abort, "the add must be on the abort path")
			expect.Equal(t, r.Step, 1, "the add must be the second step of the abort path")
		})
	})

	t.Run("Batch", func(t *testing.T) {
		t.Parallel()

		t.Run("commits the destination as the writer at the new epoch", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.flush(t)
			h.answerOne(t)
			m := h.engine.Batch()
			assert.NotNil(t, m, "the completed drop must lead to a batch")
			assert.Length(t, m.Put, 1, "the batch must put the placement")
			expect.Equal(t, m.Put[0].Replicas, []shard.Replica{primaryB}, "the destination must be the writer")
			expect.Equal(t, m.Put[0].Epoch, newEpoch, "the writer must have the new epoch")
		})

		t.Run("keeps the replicas on nodes outside the move", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			follower := shard.Replica{Node: nodeC.ID, Role: shard.RoleSecondary}
			p.Replicas = append(p.Replicas, follower)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB, nodeC)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.drain(t)
			assert.Equal(t, h.placement(t, p.Shard).Replicas, []shard.Replica{follower, primaryB},
				"the move must keep the replica on the node outside the move")
		})

		t.Run("commits the destination with the record of a move without a source", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			p.Replicas = nil
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeB)
			h.start(t, moveOf(p, shard.Node{}, nodeB, shard.Node{}))
			m := h.engine.Batch()
			assert.NotNil(t, m, "the move must lead to a batch")
			assert.Length(t, m.Transitions, 1, "the batch must commit the record")
			expect.Equal(t, m.Transitions[0].Step, 1, "the record must be at the add after the commit step")
			assert.Length(t, m.Put, 1, "the batch must put the placement")
			expect.Equal(t, m.Put[0].Replicas, []shard.Replica{primaryB}, "the destination must be the writer")
		})
	})
}
