// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package control_test

import (
	"slices"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// The times that pass between the start and the end of the writer gaps of the tests.
const (
	plannedGap = 5 * time.Second
	failureGap = 3 * time.Second
)

// firstCallbackBackoff is the backoff of the engine before the first retry of a callback,
// with the source of randomness of the tests: half of its ceiling.
const firstCallbackBackoff = 50 * time.Millisecond

// TestGap checks the writer gaps that a controller measures.
func TestGap(t *testing.T) {
	t.Parallel()

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("records a planned gap until the new writer reports the shard as serving", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.view(t, nodeA, nodeB, nodeC)
			sid := h.shardOn(t, nodeC.ID)
			h.clock.Advance(plannedGap)
			h.step(t, h.report(&nodeC, 1, healthy(sid)))
			expect.Equal(t, h.metrics.value(gapInstrument, reasonPlanned), 1.0,
				"the controller must record the gap of the move")
			expect.Equal(t, h.metrics.sum(gapInstrument, reasonPlanned), plannedGap.Seconds(),
				"the gap must last from the commit that removed the writer to the report")
			expect.Equal(t, h.metrics.value(gapInstrument, reasonFailure), 0.0,
				"the controller must not record a planned gap as a failure")
		})

		t.Run("starts a planned gap at the commit that removes the writer", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			refused := false
			h.answer = func(r *protocol.Request) error {
				if r.Call == protocol.Drop && !refused {
					refused = true
					return protocol.ErrUnreachable
				}
				return nil
			}
			h.view(t, nodeA, nodeB, nodeC)
			h.clock.Advance(plannedGap)
			h.elapse(t, firstCallbackBackoff)
			sid := h.shardOn(t, nodeC.ID)
			h.clock.Advance(plannedGap)
			h.step(t, h.report(&nodeC, 1, healthy(sid)))
			assert.Equal(t, h.metrics.value(gapInstrument, reasonPlanned), 1.0, "the controller must record the gap")
			assert.Equal(t, h.metrics.sum(gapInstrument, reasonPlanned), plannedGap.Seconds(),
				"the gap must start at the commit after the drop, and not at the first commit of the move")
		})

		t.Run("records a failure gap from the view that ends the node of the writer", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			var lost, kept []shard.Observed
			for sid, p := range h.placements(t) {
				if p.Replicas[0].Node == nodeA.ID {
					lost = append(lost, healthy(sid))
					continue
				}
				kept = append(kept, healthy(sid))
			}
			h.view(t, nodeB)
			h.clock.Advance(failureGap)
			h.step(t, h.report(&nodeB, 1, slices.Concat(lost, kept)...))
			expect.Equal(t, h.metrics.value(gapInstrument, reasonFailure), float64(len(lost)),
				"the controller must record the gap of each shard of the node that left")
			expect.Equal(t, h.metrics.sum(gapInstrument, reasonFailure), float64(len(lost))*failureGap.Seconds(),
				"each gap must last from the view to the report")
			expect.Equal(t, h.metrics.value(gapInstrument, reasonPlanned), 0.0,
				"the commit of a repair must not start a planned gap of a shard with a gap")
		})

		t.Run("records no gap for the first placement of a shard", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			for _, n := range []shard.Node{nodeA, nodeB} {
				var replicas []shard.Observed
				for sid, p := range h.placements(t) {
					if p.Replicas[0].Node == n.ID {
						replicas = append(replicas, healthy(sid))
					}
				}
				h.step(t, h.report(&n, 1, replicas...))
			}
			expect.Equal(t, h.metrics.value(gapInstrument, reasonPlanned), 0.0,
				"a shard without an earlier writer must not have a planned gap")
			expect.Equal(t, h.metrics.value(gapInstrument, reasonFailure), 0.0,
				"a shard without an earlier writer must not have a failure gap")
		})

		t.Run("ends no gap for a report of a node that is not the writer", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.view(t, nodeA, nodeB, nodeC)
			sid := h.shardOn(t, nodeC.ID)
			h.step(t, h.report(&nodeA, 1, healthy(sid)))
			assert.Equal(t, h.metrics.value(gapInstrument, reasonPlanned), 0.0,
				"the report of a node that is not the writer must not end the gap")
		})

		t.Run("ends no gap for a replica that does not serve", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.view(t, nodeA, nodeB, nodeC)
			sid := h.shardOn(t, nodeC.ID)
			starting := healthy(sid)
			starting.Status = shard.StatusStarting
			h.step(t, h.report(&nodeC, 1, starting))
			assert.Equal(t, h.metrics.value(gapInstrument, reasonPlanned), 0.0,
				"a replica that starts must not end the gap")
		})

		t.Run("records a gap once", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.view(t, nodeA, nodeB, nodeC)
			sid := h.shardOn(t, nodeC.ID)
			h.step(t, h.report(&nodeC, 1, healthy(sid)))
			h.step(t, h.report(&nodeC, 2, healthy(sid)))
			assert.Equal(t, h.metrics.value(gapInstrument, reasonPlanned), 1.0,
				"a second report must not record the gap again")
		})

		t.Run("records a gap until the commit that ends the move when the nodes do not report",
			func(t *testing.T) {
				t.Parallel()
				h := served(t, func(s *setup) { s.cfg.Unreported = true })
				refused := false
				h.answer = func(r *protocol.Request) error {
					if r.Call == protocol.Add && !refused {
						refused = true
						return protocol.ErrUnreachable
					}
					return nil
				}
				h.view(t, nodeA, nodeB, nodeC)
				h.clock.Advance(plannedGap)
				h.elapse(t, firstCallbackBackoff)
				expect.Equal(t, h.metrics.value(gapInstrument, reasonPlanned), 1.0,
					"the controller must record the gap of the move")
				expect.Equal(t, h.metrics.sum(gapInstrument, reasonPlanned), plannedGap.Seconds(),
					"the gap must last from the commit that removed the writer to the commit that ended the move")
			})

		t.Run("records the next gap of a shard as a gap of its own when the nodes do not report",
			func(t *testing.T) {
				t.Parallel()
				h := served(t, func(s *setup) { s.cfg.Unreported = true })
				h.view(t, nodeA, nodeB, nodeC)
				h.view(t, nodeA, nodeB)
				expect.Equal(t, h.metrics.value(gapInstrument, reasonPlanned), 1.0,
					"the move to the node that joined must record one planned gap")
				expect.Equal(t, h.metrics.value(gapInstrument, reasonFailure), 1.0,
					"the repair after the node left must record a failure gap of its own")
			})

		t.Run("keeps the gap of a move that ends without a writer when the nodes do not report",
			func(t *testing.T) {
				t.Parallel()
				h := served(t, func(s *setup) { s.cfg.Unreported = true })
				var lost []shard.ID
				for sid, p := range h.placements(t) {
					if p.Replicas[0].Node == nodeA.ID {
						lost = append(lost, sid)
					}
				}
				h.answer = func(r *protocol.Request) error {
					if r.Call == protocol.Add {
						h.paused = true
						return errRefused
					}
					return nil
				}
				h.view(t, nodeB)
				ps := h.placements(t)
				for _, sid := range lost {
					expect.Empty(
						t,
						ps[sid].Replicas,
						"the move of each shard of the node that left must end without a writer",
					)
				}
				assert.Equal(t, h.metrics.value(gapInstrument, reasonFailure), 0.0,
					"a move that ends without a writer must not end the gap")
			})
	})
}
