// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/shard"
)

// TestPhase checks Valid and String of every Phase.
func TestPhase(t *testing.T) {
	t.Parallel()

	t.Run("Valid", func(t *testing.T) {
		t.Parallel()

		t.Run("reports true for each of the four phases", func(t *testing.T) {
			t.Parallel()
			prop.True(t, shard.Phase.Valid, "every constant of Phase must be Valid",
				prop.Using(prop.SampledFrom(shard.PhaseRunning, shard.PhaseAborting, shard.PhaseDone,
					shard.PhaseAborted)))
		})

		t.Run("reports false for the zero Phase", func(t *testing.T) {
			t.Parallel()
			assert.False(t, shard.Phase(0).Valid(), "the zero Phase must not be Valid")
		})

		t.Run("reports false for a value above PhaseAborted", func(t *testing.T) {
			t.Parallel()
			prop.False(t, shard.Phase.Valid, "a value above the last constant must not be Valid",
				prop.Using(prop.Integer[shard.Phase](shard.PhaseAborted+1, 255)))
		})
	})

	t.Run("String", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			give shard.Phase
			want string
		}{
			{name: "returns running for PhaseRunning", give: shard.PhaseRunning, want: "running"},
			{name: "returns aborting for PhaseAborting", give: shard.PhaseAborting, want: "aborting"},
			{name: "returns done for PhaseDone", give: shard.PhaseDone, want: "done"},
			{name: "returns aborted for PhaseAborted", give: shard.PhaseAborted, want: "aborted"},
			{name: "returns the number of the zero Phase", give: 0, want: "Phase(0)"},
			{name: "returns the number of a value above PhaseAborted", give: shard.PhaseAborted + 1, want: "Phase(5)"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, tt.give.String(), tt.want, "String must return the recorded spelling of the phase")
			})
		}
	})
}

// TestPhaseAllocs checks the allocation contract of Phase. MaxAllocs counts the
// allocations of the whole process, so the test does not run in parallel.
func TestPhaseAllocs(t *testing.T) {
	phase := shard.PhaseAborting

	t.Run("Valid", func(t *testing.T) {
		var got bool
		expect.MaxAllocs(t, func() { got = phase.Valid() }, 0, "Valid must not allocate")
		assert.True(t, got, "the test must measure a Valid phase")
	})

	t.Run("String", func(t *testing.T) {
		var got string
		expect.MaxAllocs(t, func() { got = phase.String() }, 0, "String of a Valid phase must not allocate")
		assert.Equal(t, got, "aborting", "the test must measure the spelling of a Valid phase")
	})
}

// BenchmarkPhase reports the cost of Valid, which every encode and decode of a
// transition calls, and fails when it allocates.
func BenchmarkPhase(b *testing.B) {
	phase := shard.PhaseAborting

	b.Run("Valid", func(b *testing.B) {
		var got bool

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			got = phase.Valid()
		}

		assert.True(b, got, "the benchmark must measure a Valid phase")
	})
}
