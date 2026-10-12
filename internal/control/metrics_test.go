// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package control_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/handoff/breakbeforemake"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// The values of the attributes of the gauges of the reservations.
const (
	statePending  = "pending"
	stateAdmitted = "admitted"
)

// TestMetrics checks the instruments that a controller reports.
func TestMetrics(t *testing.T) {
	t.Parallel()

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("counts every shard as unplaced before the first pass", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			assert.Equal(t, h.metrics.value(unplacedInstrument), float64(shards),
				"the controller must count every shard without a writer")
		})

		t.Run("counts no shard as unplaced once every shard has a writer", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			assert.Equal(t, h.metrics.value(unplacedInstrument), 0.0,
				"the controller must not count a shard with a writer")
		})

		t.Run("counts a shard without a replica in the writer role as unplaced", func(t *testing.T) {
			t.Parallel()
			ss := spans(t)
			secondary := shard.Placement{
				Shard:    shard.ID{ID128: id.ID128{0: tagShard, 15: 1}},
				Range:    ss[0].Range,
				Replicas: []shard.Replica{{Node: nodeA.ID, Role: shard.RoleSecondary}},
				Epoch:    1,
				Reserved: 1,
				Sequence: 1,
			}
			h := newHarness(t, func(s *setup) { s.prior = []*store.Mutation{{Put: []shard.Placement{secondary}}} })
			h.start(t)
			assert.Equal(t, h.metrics.value(unplacedInstrument), float64(shards),
				"the controller must count a shard without a writer as unplaced")
		})

		t.Run("counts no shard as unplaced while writers move between nodes", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.disrupt(t, disruption(1, nodeA.ID), operator)
			assert.Equal(t, h.metrics.value(unplacedInstrument), 0.0,
				"the controller must not count a moved writer as unplaced")
		})

		t.Run("counts every pending reservation", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.disrupt(t, disruption(1, nodeA.ID), operator)
			h.disrupt(t, disruption(2, nodeB.ID), operator)
			assert.Equal(t, h.metrics.value(reservationsInstrument, statePending), 2.0,
				"the controller must count both pending reservations")
		})

		t.Run("counts no reservation once Done ends it", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			d := disruption(1, nodeA.ID)
			h.disrupt(t, d, operator)
			h.done(t, d.ID, operator)
			assert.Equal(t, h.metrics.value(reservationsInstrument, statePending), 0.0,
				"the controller must not count a reservation that has ended")
		})

		t.Run("counts the transitions in flight by protocol and phase", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.answer = func(*protocol.Request) error { return protocol.ErrUnreachable }
			h.disrupt(t, disruption(1, nodeA.ID), operator)
			got := h.metrics.value(transitionsInstrument, string(breakbeforemake.Name), shard.PhaseRunning.String())
			assert.Equal(t, got, 2.0, "the controller must count both moves off the draining node")
		})

		t.Run("sets the count of a protocol and phase without transitions to zero", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			got := h.metrics.value(transitionsInstrument, string(breakbeforemake.Name), shard.PhaseRunning.String())
			assert.Equal(t, got, 0.0, "the controller must count no transition once the first pass has ended")
		})

		t.Run("counts a reservation that the budget has not admitted as pending", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.disrupt(t, disruption(1, nodeA.ID), operator)
			expect.Equal(t, h.metrics.value(reservationsInstrument, statePending), 1.0,
				"the controller must count the pending reservation")
			expect.Equal(t, h.metrics.value(reservationsInstrument, stateAdmitted), 0.0,
				"the controller must not count a pending reservation as admitted")
		})

		t.Run("counts a reservation that the budget has admitted as admitted", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			d := disruption(1, nodeA.ID)
			h.disrupt(t, d, operator)
			h.reportAll(t)
			h.disrupt(t, d, operator)
			expect.Equal(t, h.metrics.value(reservationsInstrument, stateAdmitted), 1.0,
				"the controller must count the admitted reservation")
			expect.Equal(t, h.metrics.value(reservationsInstrument, statePending), 0.0,
				"the controller must not count an admitted reservation as pending")
		})

		t.Run("releases its instruments when it stops", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.stop(t)
			expect.True(t, h.metrics.isReleased(unplacedInstrument),
				"the controller must release the gauge of the unplaced shards")
			expect.True(t, h.metrics.isReleased(transitionsInstrument, string(breakbeforemake.Name),
				shard.PhaseRunning.String()), "the controller must release each gauge of the transitions")
			expect.True(t, h.metrics.isReleased(reservationsInstrument, statePending),
				"the controller must release the gauge of the pending reservations")
			expect.True(t, h.metrics.isReleased(reservationsInstrument, stateAdmitted),
				"the controller must release the gauge of the admitted reservations")
			expect.True(t, h.metrics.isReleased(passInstrument, modeEmergency),
				"the controller must release the histogram of the emergency passes")
			expect.True(t, h.metrics.isReleased(passInstrument, modePeriodic),
				"the controller must release the histogram of the periodic passes")
			expect.True(t, h.metrics.isReleased(movesInstrument, reasonFailure),
				"the controller must release the counter of the failures")
			expect.True(t, h.metrics.isReleased(movesInstrument, reasonDrain),
				"the controller must release the counter of the drains")
			expect.True(t, h.metrics.isReleased(movesInstrument, reasonRebalance),
				"the controller must release the counter of the rebalances")
			expect.True(t, h.metrics.isReleased(deferredInstrument),
				"the controller must release the counter of the deferrals")
			expect.True(t, h.metrics.isReleased(unknownInstrument),
				"the controller must release the counter of the commits with an unknown outcome")
			expect.True(t, h.metrics.isReleased(gapInstrument, reasonPlanned),
				"the controller must release the histogram of the planned writer gaps")
			expect.True(t, h.metrics.isReleased(gapInstrument, reasonFailure),
				"the controller must release the histogram of the failure writer gaps")
		})

		t.Run("counts each commit with an unknown outcome", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.st.FailAt(memory.PointCommitAck)
			h.start(t)
			assert.Equal(t, h.metrics.value(unknownInstrument), 1.0,
				"the controller must count the acquisition whose reply is lost")
		})
	})
}
