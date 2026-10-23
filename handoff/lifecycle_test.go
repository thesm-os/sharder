// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package handoff_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/handoff"
	"go.thesmos.sh/sharder/shard"
)

// TestLifecycle checks the table of the phases of a transition.
func TestLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("Lifecycle", func(t *testing.T) {
		t.Parallel()

		t.Run("starts a transition in PhaseRunning", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, handoff.Lifecycle.Initial(), shard.PhaseRunning,
				"every transition must start in the running phase")
		})

		// The rows cover the 16 pairs of one of the four phases and one of the four events,
		// and two values outside the events. A row with ok false is an event that the phase
		// rejects, and want is then the same phase.
		tests := []struct {
			name string
			from shard.Phase
			on   handoff.Event
			want shard.Phase
			ok   bool
		}{
			{
				name: "keeps a running transition running on Advance",
				from: shard.PhaseRunning, on: handoff.Advance, want: shard.PhaseRunning, ok: true,
			},
			{
				name: "moves a running transition to PhaseDone on Finish",
				from: shard.PhaseRunning, on: handoff.Finish, want: shard.PhaseDone, ok: true,
			},
			{
				name: "moves a running transition to PhaseAborting on Fail",
				from: shard.PhaseRunning, on: handoff.Fail, want: shard.PhaseAborting, ok: true,
			},
			{
				name: "rejects Abandon in a running transition",
				from: shard.PhaseRunning, on: handoff.Abandon, want: shard.PhaseRunning,
			},
			{
				name: "keeps an aborting transition aborting on Advance",
				from: shard.PhaseAborting, on: handoff.Advance, want: shard.PhaseAborting, ok: true,
			},
			{
				name: "moves an aborting transition to PhaseAborted on Finish",
				from: shard.PhaseAborting, on: handoff.Finish, want: shard.PhaseAborted, ok: true,
			},
			{
				name: "keeps an aborting transition aborting on Fail",
				from: shard.PhaseAborting, on: handoff.Fail, want: shard.PhaseAborting, ok: true,
			},
			{
				name: "moves an aborting transition to PhaseAborted on Abandon",
				from: shard.PhaseAborting, on: handoff.Abandon, want: shard.PhaseAborted, ok: true,
			},
			{
				name: "rejects Advance in a done transition",
				from: shard.PhaseDone, on: handoff.Advance, want: shard.PhaseDone,
			},
			{
				name: "rejects Finish in a done transition",
				from: shard.PhaseDone, on: handoff.Finish, want: shard.PhaseDone,
			},
			{
				name: "rejects Fail in a done transition",
				from: shard.PhaseDone, on: handoff.Fail, want: shard.PhaseDone,
			},
			{
				name: "rejects Abandon in a done transition",
				from: shard.PhaseDone, on: handoff.Abandon, want: shard.PhaseDone,
			},
			{
				name: "rejects Advance in an aborted transition",
				from: shard.PhaseAborted, on: handoff.Advance, want: shard.PhaseAborted,
			},
			{
				name: "rejects Finish in an aborted transition",
				from: shard.PhaseAborted, on: handoff.Finish, want: shard.PhaseAborted,
			},
			{
				name: "rejects Fail in an aborted transition",
				from: shard.PhaseAborted, on: handoff.Fail, want: shard.PhaseAborted,
			},
			{
				name: "rejects Abandon in an aborted transition",
				from: shard.PhaseAborted, on: handoff.Abandon, want: shard.PhaseAborted,
			},
			{
				name: "rejects the zero Event in a running transition",
				from: shard.PhaseRunning, on: 0, want: shard.PhaseRunning,
			},
			{
				name: "rejects an event above Abandon in an aborting transition",
				from: shard.PhaseAborting, on: handoff.Abandon + 1, want: shard.PhaseAborting,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				got, ok := handoff.Lifecycle.Next(tt.from, tt.on, nil)
				expect.Equal(t, got, tt.want, "the event must lead to its phase")
				expect.Equal(t, ok, tt.ok, "the phase must accept exactly the events of its edges")
			})
		}

		t.Run("ends a transition in PhaseDone and PhaseAborted", func(t *testing.T) {
			t.Parallel()
			prop.True(t, handoff.Lifecycle.Terminal, "done and aborted must be terminal",
				prop.Using(prop.SampledFrom(shard.PhaseDone, shard.PhaseAborted)))
		})

		t.Run("continues a running and an aborting transition", func(t *testing.T) {
			t.Parallel()
			prop.False(t, handoff.Lifecycle.Terminal, "running and aborting must not be terminal",
				prop.Using(prop.SampledFrom(shard.PhaseRunning, shard.PhaseAborting)))
		})
	})
}
