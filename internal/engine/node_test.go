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

// TestNode checks the permits of the callbacks to one node, and the queue of the callbacks
// that wait for a permit.
func TestNode(t *testing.T) {
	t.Parallel()

	// queued returns a harness with one permit per node, whose Engine has started the moves
	// of n shards from node A and sent the first drop to A.
	queued := func(tb testCase, n int) *harness {
		tb.Helper()
		ps := placements(n, nodeA)
		h := newHarness(tb, ps, func(s *setup) { s.cfg.NodeCallbacks = 1 })
		h.view(tb, nodeA, nodeB, nodeC)
		for _, p := range ps {
			h.start(tb, moveOf(p, nodeA, nodeB, nodeA))
		}
		h.flush(tb)
		return h
	}

	t.Run("Applied", func(t *testing.T) {
		t.Parallel()

		t.Run("sends at most NodeCallbacks callbacks to one node", func(t *testing.T) {
			t.Parallel()
			h := queued(t, 2)
			assert.Length(t, h.calls, 1, "the node must have one callback in flight")
		})
	})

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("sends a queued callback once a callback of its node returns", func(t *testing.T) {
			t.Parallel()
			h := queued(t, 2)
			first := h.calls[0].Request.Shard
			h.answerOne(t)
			assert.Length(t, h.calls, 1, "the returned permit must send the queued drop")
			expect.NotEqual(t, h.calls[0].Request.Shard, first, "the queued drop must be the drop of the second move")
		})

		t.Run("sends the queued callbacks of a node in the order of the queue", func(t *testing.T) {
			t.Parallel()
			h := queued(t, 3)
			for h.answerOne(t) {
			}
			shards := make([]shard.ID, 0, len(h.sent))
			for _, r := range h.sent {
				shards = append(shards, r.Shard)
			}
			want := placements(3, nodeA)
			assert.Equal(t, shards, []shard.ID{want[0].Shard, want[1].Shard, want[2].Shard},
				"the drops must leave the queue in the order of the moves")
		})

		t.Run("returns the permit of a callback that fails", func(t *testing.T) {
			t.Parallel()
			h := queued(t, 2)
			h.answer = refuse(protocol.Drop, "")
			h.answerOne(t)
			assert.Length(t, h.calls, 1, "the failed drop must return its permit to the queued drop")
		})
	})

	t.Run("View", func(t *testing.T) {
		t.Parallel()

		t.Run("removes a queued callback whose node is not in the first view", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.flush(t)
			h.view(t, nodeB)
			h.drain(t)
			assert.Equal(t, calls(h.sent), []call{{protocol.Add, nodeB.ID}},
				"the queued drop must complete without a callback to the node that left")
		})

		t.Run("removes the queued callbacks of an incarnation that has ended", func(t *testing.T) {
			t.Parallel()
			h := queued(t, 2)
			h.calls = nil
			h.view(t, restarted(nodeA), nodeB, nodeC)
			expect.Empty(t, h.calls, "the Engine must not send a drop to the ended incarnation")
			h.drain(t)
			expect.Equal(t, calls(h.sent), []call{
				{protocol.Drop, nodeA.ID}, {protocol.Add, nodeB.ID}, {protocol.Add, nodeB.ID},
			}, "both drops must complete with the end of the incarnation")
		})

		t.Run("keeps the queued callback of the live incarnation of the node", func(t *testing.T) {
			t.Parallel()
			ps := placements(2, nodeA)
			h := newHarness(t, ps)
			h.start(t, moveOf(ps[0], restarted(nodeA), nodeB, restarted(nodeA)))
			h.start(t, moveOf(ps[1], nodeA, nodeB, nodeA))
			h.flush(t)
			h.view(t, restarted(nodeA), nodeB)
			assert.Length(t, h.calls, 1, "the first view must send one drop")
			expect.Equal(t, h.calls[0].Request.NodeIncarnation, restarted(nodeA).Incarnation,
				"the drop must address the live incarnation")
		})

		t.Run("completes a queued drop of an ended incarnation behind a live callback", func(t *testing.T) {
			t.Parallel()
			ps := placements(2, nodeA)
			h := newHarness(t, ps, func(s *setup) { s.cfg.NodeCallbacks = 1 })
			h.start(t, moveOf(ps[0], restarted(nodeA), nodeB, restarted(nodeA)))
			h.start(t, moveOf(ps[1], nodeA, nodeB, nodeA))
			h.flush(t)
			h.view(t, restarted(nodeA), nodeB)
			assert.True(t, h.flush(t), "the first view must complete the drop of the ended incarnation")
		})

		t.Run("removes a queued retry of an incarnation that has ended", func(t *testing.T) {
			t.Parallel()
			h := queued(t, 2)
			h.answer = failures(1, protocol.ErrUnreachable)
			h.answerOne(t)
			h.elapseOne(t)
			assert.Length(t, h.calls, 1, "the retry must wait behind the second drop")
			h.calls = nil
			h.view(t, restarted(nodeA), nodeB, nodeC)
			assert.Empty(t, h.calls, "the Engine must not send the next step of the retry before its batch")
		})
	})
}
