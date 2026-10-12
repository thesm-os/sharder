// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent_test

import (
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/agent"
	"go.thesmos.sh/sharder/shard"
)

// maxCause is the most bytes of the message of the cause of a fault that a report carries.
const maxCause = 1024

// errDisk is the cause of the faults of the tests.
var errDisk = errors.New("test: the disk fails")

// TestFault checks the faults that the application reports in the replicas of the node.
func TestFault(t *testing.T) {
	t.Parallel()

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("reports a replica with a fault as failed at once", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			to := h.fault(t, shardA, shard.FaultPermanent, errDisk)
			expect.NoError(t, replyTo[struct{}](t, h, to).Err, "the agent must take the fault")
			got := h.observed(t, shardA)
			expect.Equal(t, got.Status, shard.StatusFailed, "the replica must be failed")
			expect.Equal(t, got.Fault, shard.FaultPermanent, "the report must have the fault")
			expect.Equal(t, got.Cause, errDisk.Error(), "the report must have the message of the cause")
		})

		t.Run("reports a fault without a cause", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.fault(t, shardA, shard.FaultTransient, nil)
			assert.Empty(t, h.observed(t, shardA).Cause, "a fault without a cause must have no cause")
		})

		t.Run("reports the first 1,024 bytes of the cause", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.fault(t, shardA, shard.FaultPermanent, errors.New(strings.Repeat("x", 2*maxCause)))
			assert.Equal(t, h.observed(t, shardA).Cause, strings.Repeat("x", maxCause), "the cause must be cut")
		})

		t.Run("keeps the cause valid UTF-8 when it cuts a character", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.fault(t, shardA, shard.FaultPermanent, errors.New(strings.Repeat("x", maxCause-1)+"é"))
			got := h.observed(t, shardA).Cause
			expect.True(t, utf8.ValidString(got), "the cause must be valid UTF-8")
			expect.Equal(t, got, strings.Repeat("x", maxCause-1), "the cause must end before the cut character")
		})

		t.Run("returns ErrRequest for a fault that is not a fault", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			to := h.fault(t, shardA, shard.FaultNone, errDisk)
			assert.ErrorIs(t, replyTo[struct{}](t, h, to).Err, agent.ErrRequest, "the agent must refuse no fault")
		})

		t.Run("returns ErrNoReplica for a fault of a shard without a replica", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			to := h.fault(t, shardA, shard.FaultPermanent, errDisk)
			assert.ErrorIs(t, replyTo[struct{}](t, h, to).Err, agent.ErrNoReplica,
				"the agent must refuse the fault of a replica that it does not have")
		})

		t.Run("serves a replica again after it recovers from a transient fault", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.fault(t, shardA, shard.FaultTransient, errDisk)
			to := h.recover(t, shardA)
			expect.NoError(t, replyTo[struct{}](t, h, to).Err, "the agent must take the recovery")
			got := h.observed(t, shardA)
			expect.Equal(t, got.Status, shard.StatusServing, "the replica must serve")
			expect.Equal(t, got.Fault, shard.FaultNone, "the replica must have no fault")
			expect.Empty(t, got.Cause, "the replica must have no cause")
		})

		t.Run("returns ErrNoFault for the recovery of a replica with a permanent fault", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.fault(t, shardA, shard.FaultPermanent, errDisk)
			to := h.recover(t, shardA)
			assert.ErrorIs(t, replyTo[struct{}](t, h, to).Err, agent.ErrNoFault,
				"a permanent fault must not recover")
		})

		t.Run("returns ErrNoReplica for the recovery of a shard without a replica", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			to := h.recover(t, shardA)
			assert.ErrorIs(t, replyTo[struct{}](t, h, to).Err, agent.ErrNoReplica,
				"the agent must refuse the recovery of a replica that it does not have")
		})
	})
}

// fault passes the report of the fault f in the replica of the shard sid to the agent, with
// the error of the application in cause, and runs the agent. It returns the token of the call.
func (h *harness) fault(tb assert.TB, sid shard.ID, f shard.Fault, cause error) machine.ReplyToken {
	tb.Helper()
	to := h.call()
	h.step(tb, machine.ReportFault{Cause: cause, Reply: to, Shard: sid, Fault: f})
	h.run(tb)
	return to
}

// recover passes the recovery of the replica of the shard sid to the agent, and runs the
// agent. It returns the token of the call.
func (h *harness) recover(tb assert.TB, sid shard.ID) machine.ReplyToken {
	tb.Helper()
	to := h.call()
	h.step(tb, machine.Recovered{Reply: to, Shard: sid})
	h.run(tb)
	return to
}
