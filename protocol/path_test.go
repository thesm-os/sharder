// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package protocol_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/protocol"
)

// TestPath checks Valid and String of every Path, and the refusal of a Path that is not
// Valid by the codec.
func TestPath(t *testing.T) {
	t.Parallel()

	t.Run("Valid", func(t *testing.T) {
		t.Parallel()

		t.Run("reports true for each of the three paths", func(t *testing.T) {
			t.Parallel()
			prop.True(t, protocol.Path.Valid, "every constant of Path must be Valid",
				prop.Using(prop.SampledFrom(protocol.Forward, protocol.Abort, protocol.Cleanup)))
		})

		t.Run("reports false for the zero Path", func(t *testing.T) {
			t.Parallel()
			assert.False(t, protocol.Path(0).Valid(), "the zero Path must not be Valid")
		})

		t.Run("reports false for a value above Cleanup", func(t *testing.T) {
			t.Parallel()
			prop.False(t, protocol.Path.Valid, "a value above the last constant must not be Valid",
				prop.Using(prop.Integer[protocol.Path](protocol.Cleanup+1, 255)))
		})
	})

	t.Run("String", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			give protocol.Path
			want string
		}{
			{name: "returns forward for Forward", give: protocol.Forward, want: "forward"},
			{name: "returns abort for Abort", give: protocol.Abort, want: "abort"},
			{name: "returns cleanup for Cleanup", give: protocol.Cleanup, want: "cleanup"},
			{name: "returns the number of the zero Path", give: 0, want: "Path(0)"},
			{name: "returns the number of a value above Cleanup", give: protocol.Cleanup + 1, want: "Path(4)"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, tt.give.String(), tt.want, "String must return the recorded spelling of the path")
			})
		}
	})

	t.Run("ValidateKanon", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrUnknownPath for a Path that is not Valid", func(t *testing.T) {
			t.Parallel()
			prop.ErrorIs(t, func(p protocol.Path) error { return p.ValidateKanon() }, protocol.ErrUnknownPath,
				"the codec must refuse a path outside the three",
				prop.Using(prop.Integer[protocol.Path](protocol.Cleanup+1, 255)))
		})

		t.Run("returns nil for a Valid Path", func(t *testing.T) {
			t.Parallel()
			prop.NoError(t, func(p protocol.Path) error { return p.ValidateKanon() }, "the codec must accept each path",
				prop.Using(prop.SampledFrom(protocol.Forward, protocol.Abort, protocol.Cleanup)))
		})
	})
}

// TestPathAllocs checks the allocation contract of Path. MaxAllocs counts the allocations of
// the whole process, so the test does not run in parallel.
func TestPathAllocs(t *testing.T) {
	path := protocol.Abort

	t.Run("Valid", func(t *testing.T) {
		var got bool
		expect.MaxAllocs(t, func() { got = path.Valid() }, 0, "Valid must not allocate")
		assert.True(t, got, "the test must measure a Valid path")
	})

	t.Run("String", func(t *testing.T) {
		var got string
		expect.MaxAllocs(t, func() { got = path.String() }, 0, "String of a Valid path must not allocate")
		assert.Equal(t, got, "abort", "the test must measure the spelling of a Valid path")
	})
}

// BenchmarkPath reports the cost of Valid, which every encode and decode of a request
// calls, and fails when it allocates.
func BenchmarkPath(b *testing.B) {
	path := protocol.Abort

	b.Run("Valid", func(b *testing.B) {
		var got bool

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			got = path.Valid()
		}

		assert.True(b, got, "the benchmark must measure a Valid path")
	})
}
