// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/history"
	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/agent"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// generations is the number of the generations of the scope of the cell that a spec of
// Observed commits while a reader calls Observed.
const generations = 20

// TestObserved checks the observation of its cell that an agent publishes.
func TestObserved(t *testing.T) {
	t.Parallel()

	t.Run("Observed", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrNotServing before the agent serves", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			_, err := h.machine.Observed()
			assert.ErrorIs(t, err, agent.ErrNotServing, "an agent that does not serve must have no observation")
		})

		t.Run("returns the cell and the incarnation of the node once the agent serves", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			got, err := h.machine.Observed()
			assert.NoError(t, err, "a serving agent must have an observation")
			expect.Equal(t, got.Cell, cellID, "the observation must name the cell of the node")
			expect.Equal(t, got.Incarnation, incA, "the observation must name the namespace incarnation")
			expect.Equal(t, got.Generation, h.state(t).Head.Generation,
				"the observation must have the generation of the snapshot")
		})

		t.Run("returns the last generation that the agent has applied", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			gen := h.commitCell(t, &store.Mutation{Put: []shard.Placement{placement(shardB, firstEpoch, otherNode)}})
			h.follow(t)
			got, err := h.machine.Observed()
			assert.NoError(t, err, "a serving agent must have an observation")
			assert.Equal(t, got.Generation, gen, "the observation must have the generation of the last commit")
		})

		t.Run("returns a generation that does not decrease while the agent steps", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			done := make(chan struct{})
			var seen []epoch.Epoch
			outcomes := history.Concurrently(2, within, func(client int) (any, error) {
				if client == 0 {
					defer close(done)
					for e := range epoch.Epoch(generations) {
						h.unplace(t, shardB, firstEpoch+e)
					}
					return nil, nil
				}
				for {
					got, err := h.machine.Observed()
					if err != nil {
						return nil, err
					}
					seen = append(seen, got.Generation)
					select {
					case <-done:
						return nil, nil
					default:
					}
				}
			})
			for _, o := range outcomes {
				assert.True(t, o.Finished, "every client of the spec must finish")
				assert.NoError(t, o.Error, "every call of Observed must return the observation")
			}
			assert.Pairwise(t, seen, func(earlier, later epoch.Epoch) bool { return earlier <= later },
				"the generation of the observation must not decrease")
		})

		t.Run("returns ErrNotServing after the agent has stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.step(t, machine.Stopped{})
			_, err := h.machine.Observed()
			assert.ErrorIs(t, err, agent.ErrNotServing, "a stopped agent must have no observation")
		})
	})
}

// TestObservedAllocs checks that Observed does not allocate.
func TestObservedAllocs(t *testing.T) {
	h := newHarness(t)
	h.start(t)
	m := h.machine

	t.Run("Observed", func(t *testing.T) {
		var err error
		expect.MaxAllocs(t, func() { _, err = m.Observed() }, 0, "Observed must not allocate")
		expect.NoError(t, err, "the measurement must take the observation of a serving agent")
	})
}

// BenchmarkObserved measures Observed.
func BenchmarkObserved(b *testing.B) {
	h := newHarness(b)
	h.start(b)
	m := h.machine

	b.Run("Observed", func(b *testing.B) {
		c := bench.Start(b).MaxAllocs(0)
		defer c.End()
		var got agent.Observation
		var err error
		for c.Loop() {
			got, err = m.Observed()
		}
		assert.NoError(b, err, "the benchmark must measure a serving agent")
		assert.Equal(b, got.Cell, cellID, "the benchmark must return the cell of the node")
	})
}
