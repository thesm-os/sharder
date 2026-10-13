// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/id/ulid"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/agent"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// The cells of the root scope of the tests other than the cell of the tests.
var (
	// secondKey is the 16 bytes of a second cell, which starts after the cell of the tests.
	secondKey = id.ID128{0: 0x01, 15: 0xcf}
	// thirdKey is the 16 bytes of a third cell, which starts after the second cell.
	thirdKey = id.ID128{0: 0x01, 15: 0xd0}
)

// TestNamespace checks what an agent reads from the root scope before it joins its cell.
func TestNamespace(t *testing.T) {
	t.Parallel()

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("waits for the root to create the namespace", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.create = false })
			started := h.call()
			h.step(t, machine.Started{Reply: started})
			h.run(t)
			h.follow(t)
			expect.Empty(t, h.joins, "the agent must wait for the namespace")
			h.commitRoot(t, &h.root)
			h.follow(t)
			expect.Length(t, h.joins, 1, "the agent must join once the root creates the namespace")
			expect.Equal(t, h.joins[0].Incarnation, incA, "the agent must take the incarnation of the namespace")
			expect.NoError(t, replyTo[struct{}](t, h, started).Err, "the agent must serve")
		})

		t.Run("waits for the root to place the cell", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.root.Put = nil })
			h.start(t)
			expect.Empty(t, h.joins, "the agent must wait for its cell")
			h.commitRoot(t, &store.Mutation{Put: []shard.Placement{{Shard: shard.ID{ID128: cellKey}}}})
			h.follow(t)
			expect.Length(t, h.joins, 1, "the agent must join once the root places the cell")
		})

		t.Run("keeps one cell that the root places again", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.create = false })
			cell := shard.Placement{Shard: shard.ID{ID128: cellKey}}
			h.commitRoot(t, &store.Mutation{Put: []shard.Placement{cell}})
			h.start(t)
			expect.Empty(t, h.joins, "the agent must wait for the spec of the namespace")
			h.commitRoot(t, &store.Mutation{Spec: h.root.Spec, Put: []shard.Placement{cell}})
			h.follow(t)
			expect.Length(t, h.joins, 1, "a cell that the root places twice must remain one cell")
			expect.Nil(t, h.machine.Err(), "the agent must join the one cell")
		})

		t.Run("joins the cell that the node names", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) {
				s.root.Put = append(s.root.Put, shard.Placement{
					Shard: shard.ID{ID128: secondKey}, Range: shard.Range{Start: []byte{0xf0}},
				})
				s.cfg.Node.Cell = cellID
			})
			h.start(t)
			assert.Length(t, h.joins, 1, "the agent must join its cell")
			assert.Equal(t, h.joins[0].Scope, cellScope, "the agent must join the cell that the node names")
		})

		t.Run("finds the cell of the node on a later page of the root scope", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) {
				s.root.Put = append(s.root.Put,
					shard.Placement{Shard: shard.ID{ID128: secondKey}, Range: shard.Range{Start: []byte{0xf0}}},
					shard.Placement{Shard: shard.ID{ID128: thirdKey}, Range: shard.Range{Start: []byte{0xf1}}},
				)
				s.cfg.Node.Cell = shard.CellID(ulid.Format(thirdKey.ID()))
			})
			h.start(t)
			assert.Length(t, h.joins, 1, "the agent must find its cell on the last page")
			assert.Equal(t, h.joins[0].Scope.Cell, shard.CellID(ulid.Format(thirdKey.ID())),
				"the agent must join the cell that the node names")
		})

		t.Run("returns ErrConfig for a cell that the namespace does not have", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.cfg.Node.Cell = shard.CellID(ulid.Format(secondKey.ID())) })
			h.start(t)
			assert.ErrorIs(t, h.machine.Err(), agent.ErrConfig, "the agent must stop for a cell that does not exist")
		})

		t.Run("returns ErrConfig for a node without a cell in a namespace of two cells", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) {
				s.root.Put = append(s.root.Put, shard.Placement{
					Shard: shard.ID{ID128: secondKey}, Range: shard.Range{Start: []byte{0xf0}},
				})
			})
			h.start(t)
			assert.ErrorIs(t, h.machine.Err(), agent.ErrConfig, "a node without a cell must have one cell to join")
		})

		t.Run("joins the root scope as the agent of the root", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, rooted)
			h.start(t)
			assert.Length(t, h.joins, 1, "the agent of the root must join once")
			assert.Equal(t, h.joins[0].Scope, rootScope, "the agent of the root must join the root scope")
		})

		t.Run("watches the root scope for its node as the agent of the root", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, rooted)
			h.start(t)
			assert.Length(t, h.watches, 1, "the agent of the root must watch one scope")
			expect.Equal(t, h.watches[0].Scope, rootScope, "the agent of the root must watch the root scope")
			expect.Equal(t, h.watches[0].Node, node.ID, "the watch must select the changes of the node")
		})

		t.Run("joins the root scope of a namespace without a cell as the agent of the root", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, rooted, func(s *setup) { s.root.Put = nil })
			h.start(t)
			assert.Length(t, h.joins, 1, "the agent of the root must not wait for a cell")
		})

		t.Run("stops when a read of the root scope fails", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			assert.NoError(t, h.st.Close(), "the store must close")
			h.start(t)
			assert.NotNil(t, h.machine.Err(), "the agent must stop when it cannot read the root scope")
		})
	})
}
