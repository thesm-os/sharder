// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/protocol"
)

// TestMetrics checks the instruments of an agent.
func TestMetrics(t *testing.T) {
	t.Parallel()

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("counts a fence acquisition that returns epoch.ErrFenced", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.fence = func(*fence.Token) error { return epoch.ErrFenced }
			h.place(t, shardA, firstEpoch)
			h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			assert.Equal(t, h.metrics.values[refusalsInstrument], 1.0, "the agent must count the refusal of the fence")
		})

		t.Run("does not count a fence acquisition that fails with another error", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.fence = func(*fence.Token) error { return protocol.ErrUnreachable }
			h.place(t, shardA, firstEpoch)
			h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			assert.Equal(
				t,
				h.metrics.values[refusalsInstrument],
				0.0,
				"the agent must count refusals of the fence alone",
			)
		})
	})
}
