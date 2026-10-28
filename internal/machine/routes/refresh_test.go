// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package routes_test

import (
	"testing"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/routes"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// unknownCell is a cell that the cell map of the tests does not have.
const unknownCell shard.CellID = "unknown"

// TestRefresh checks the waits of the Refresh calls of clients.
func TestRefresh(t *testing.T) {
	t.Parallel()

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("replies at once to a Refresh of a generation that the router has applied", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			to := h.refresh(t, incA, cellID, h.placed)
			assert.Nil(t, h.replied(t, to).Err, "the router must reply to a generation that it has applied")
		})

		t.Run("replies to a Refresh once the router applies its generation", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			to := h.refresh(t, incA, cellID, h.placed+1)
			_, ok := h.replyTo(to)
			assert.False(t, ok, "the router must wait for a generation that it has not applied")
			h.commit(t, cellScope, &store.Mutation{Put: []shard.Placement{h.placement(1, nodeB.ID, firstEpoch+1)}})
			h.follow(t)
			assert.Nil(t, h.replied(t, to).Err, "the router must reply once it has applied the generation")
		})

		t.Run("returns ErrCell for a cell that the cell map does not have", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			to := h.refresh(t, incA, unknownCell, h.placed)
			assert.ErrorIs(
				t,
				h.replied(t, to).Err,
				routes.ErrCell,
				"the router must refuse a cell that it does not have",
			)
		})

		t.Run("returns shard.ErrIncarnation for another incarnation once it has read the root", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.pause = func(machine.Effect) bool { return true }
			to := h.refresh(t, incB, cellID, h.placed)
			_, ok := h.replyTo(to)
			assert.False(t, ok, "the router must wait for its read of the root scope")
			h.pause = nil
			h.run(t)
			assert.ErrorIs(t, h.replied(t, to).Err, shard.ErrIncarnation,
				"the router must refuse another incarnation than the installed one")
		})

		t.Run("returns shard.ErrIncarnation to a Refresh that waits when it installs another incarnation",
			func(t *testing.T) {
				t.Parallel()
				h := newHarness(t)
				h.start(t)
				to := h.refresh(t, incA, cellID, h.placed+1)
				recreated := h.recreate(t, incB, recreatedKey)
				h.follow(t)
				h.view(t, recreated, nodeA)
				assert.ErrorIs(t, h.replied(t, to).Err, shard.ErrIncarnation,
					"a Refresh of the old incarnation must end when the new one is installed")
			})

		t.Run("waits for its generation in the incarnation that a rediscovery installs", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			recreated := h.recreate(t, incB, recreatedKey)
			to := h.refresh(t, incB, recreated.Cell, 1)
			_, ok := h.replyTo(to)
			assert.False(t, ok, "the router must wait for the install of the new incarnation")
			h.view(t, recreated, nodeA)
			assert.Nil(t, h.replied(t, to).Err, "the router must reply once it has installed the incarnation")
		})

		t.Run("waits for the first read of the namespace", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.step(t, machine.Started{Reply: h.call()})
			to := h.refresh(t, incA, cellID, h.placed)
			_, ok := h.replyTo(to)
			assert.False(t, ok, "the router must wait for its first read")
			h.view(t, cellScope, nodeA)
			assert.Nil(t, h.replied(t, to).Err, "the router must reply once it has read the namespace")
		})

		t.Run("returns ErrMapping for a namespace whose positions it cannot compute", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.st.Delete(rootScope)
			h.incarnation = incB
			clear(h.seqs)
			recorded := h.mapping
			recorded.Config = []byte(eightSlots)
			h.commit(t, rootScope, h.creation(recreatedKey, recorded))
			h.follow(t)
			to := h.refresh(t, incB, cellID, 1)
			assert.ErrorIs(t, h.replied(t, to).Err, routes.ErrMapping,
				"the router must refuse a Refresh of a namespace that it cannot compute")
		})

		t.Run("drops a Refresh that the runtime cancelled", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			to := h.refresh(t, incA, cellID, h.placed+1)
			h.step(t, machine.Cancelled{Reply: to})
			h.commit(t, cellScope, &store.Mutation{Put: []shard.Placement{h.placement(1, nodeB.ID, firstEpoch+1)}})
			h.follow(t)
			_, ok := h.replyTo(to)
			assert.False(t, ok, "the router must not reply to a call that the runtime cancelled")
		})
	})
}
