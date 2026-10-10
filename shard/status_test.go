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

// TestStatus checks Valid and String of every Status.
func TestStatus(t *testing.T) {
	t.Parallel()

	t.Run("Valid", func(t *testing.T) {
		t.Parallel()

		t.Run("reports true for each of the four statuses", func(t *testing.T) {
			t.Parallel()
			prop.True(
				t,
				shard.Status.Valid,
				"every constant of Status must be Valid",
				prop.Using(
					prop.SampledFrom(
						shard.StatusStarting,
						shard.StatusServing,
						shard.StatusStopping,
						shard.StatusFailed,
					),
				),
			)
		})

		t.Run("reports false for the zero Status", func(t *testing.T) {
			t.Parallel()
			assert.False(t, shard.Status(0).Valid(), "the zero Status must not be Valid")
		})

		t.Run("reports false for a value above StatusFailed", func(t *testing.T) {
			t.Parallel()
			prop.False(t, shard.Status.Valid, "a value above the last constant must not be Valid",
				prop.Using(prop.Integer[shard.Status](shard.StatusFailed+1, 255)))
		})
	})

	t.Run("String", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			give shard.Status
			want string
		}{
			{name: "returns starting for StatusStarting", give: shard.StatusStarting, want: "starting"},
			{name: "returns serving for StatusServing", give: shard.StatusServing, want: "serving"},
			{name: "returns stopping for StatusStopping", give: shard.StatusStopping, want: "stopping"},
			{name: "returns failed for StatusFailed", give: shard.StatusFailed, want: "failed"},
			{name: "returns the number of the zero Status", give: 0, want: "Status(0)"},
			{name: "returns the number of a value above StatusFailed", give: shard.StatusFailed + 1, want: "Status(5)"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, tt.give.String(), tt.want, "String must return the recorded spelling of the status")
			})
		}
	})
}

// TestStatusAllocs checks the allocation contract of Status. MaxAllocs counts the
// allocations of the whole process, so the test does not run in parallel.
func TestStatusAllocs(t *testing.T) {
	status := shard.StatusServing

	t.Run("Valid", func(t *testing.T) {
		var got bool
		expect.MaxAllocs(t, func() { got = status.Valid() }, 0, "Valid must not allocate")
		assert.True(t, got, "the test must measure a Valid status")
	})

	t.Run("String", func(t *testing.T) {
		var got string
		expect.MaxAllocs(t, func() { got = status.String() }, 0, "String of a Valid status must not allocate")
		assert.Equal(t, got, "serving", "the test must measure the spelling of a Valid status")
	})
}

// BenchmarkStatus reports the cost of Valid, which every encode and decode of a status
// calls, and fails when it allocates.
func BenchmarkStatus(b *testing.B) {
	status := shard.StatusServing

	b.Run("Valid", func(b *testing.B) {
		var got bool

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			got = status.Valid()
		}

		assert.True(b, got, "the benchmark must measure a Valid status")
	})
}
