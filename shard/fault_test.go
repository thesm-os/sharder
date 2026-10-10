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

// TestFault checks Valid and String of every Fault.
func TestFault(t *testing.T) {
	t.Parallel()

	t.Run("Valid", func(t *testing.T) {
		t.Parallel()

		t.Run("reports true for each of the three faults", func(t *testing.T) {
			t.Parallel()
			prop.True(t, shard.Fault.Valid, "every constant of Fault must be Valid",
				prop.Using(prop.SampledFrom(shard.FaultNone, shard.FaultTransient, shard.FaultPermanent)))
		})

		t.Run("reports false for a value above FaultPermanent", func(t *testing.T) {
			t.Parallel()
			prop.False(t, shard.Fault.Valid, "a value above the last constant must not be Valid",
				prop.Using(prop.Integer[shard.Fault](shard.FaultPermanent+1, 255)))
		})
	})

	t.Run("String", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			give shard.Fault
			want string
		}{
			{name: "returns none for FaultNone", give: shard.FaultNone, want: "none"},
			{name: "returns transient for FaultTransient", give: shard.FaultTransient, want: "transient"},
			{name: "returns permanent for FaultPermanent", give: shard.FaultPermanent, want: "permanent"},
			{name: "returns the number of a value that is not Valid", give: shard.FaultPermanent + 1, want: "Fault(3)"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, tt.give.String(), tt.want, "String must return the recorded spelling of the fault")
			})
		}
	})
}

// TestFaultAllocs checks the allocation contract of Fault. MaxAllocs counts the
// allocations of the whole process, so the test does not run in parallel.
func TestFaultAllocs(t *testing.T) {
	fault := shard.FaultTransient

	t.Run("Valid", func(t *testing.T) {
		var got bool
		expect.MaxAllocs(t, func() { got = fault.Valid() }, 0, "Valid must not allocate")
		assert.True(t, got, "the test must measure a Valid fault")
	})

	t.Run("String", func(t *testing.T) {
		var got string
		expect.MaxAllocs(t, func() { got = fault.String() }, 0, "String of a Valid fault must not allocate")
		assert.Equal(t, got, "transient", "the test must measure the spelling of a Valid fault")
	})
}

// BenchmarkFault reports the cost of Valid, which every encode and decode of a fault
// calls, and fails when it allocates.
func BenchmarkFault(b *testing.B) {
	fault := shard.FaultTransient

	b.Run("Valid", func(b *testing.B) {
		var got bool

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			got = fault.Valid()
		}

		assert.True(b, got, "the benchmark must measure a Valid fault")
	})
}
