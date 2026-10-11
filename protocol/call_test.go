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

// TestCall checks Valid and String of every Call, and the refusal of a Call that is not
// Valid by the codec of a Request.
func TestCall(t *testing.T) {
	t.Parallel()

	t.Run("Valid", func(t *testing.T) {
		t.Parallel()

		t.Run("reports true for Add and Drop", func(t *testing.T) {
			t.Parallel()
			prop.True(t, protocol.Call.Valid, "every constant of Call must be Valid",
				prop.Using(prop.SampledFrom(protocol.Add, protocol.Drop)))
		})

		t.Run("reports false for the zero Call", func(t *testing.T) {
			t.Parallel()
			assert.False(t, protocol.Call(0).Valid(), "the zero Call must not be Valid")
		})

		t.Run("reports false for a value above Drop", func(t *testing.T) {
			t.Parallel()
			prop.False(t, protocol.Call.Valid, "a value above the last constant must not be Valid",
				prop.Using(prop.Integer[protocol.Call](protocol.Drop+1, 255)))
		})
	})

	t.Run("String", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			give protocol.Call
			want string
		}{
			{name: "returns add for Add", give: protocol.Add, want: "add"},
			{name: "returns drop for Drop", give: protocol.Drop, want: "drop"},
			{name: "returns the number of the zero Call", give: 0, want: "Call(0)"},
			{name: "returns the number of a value above Drop", give: protocol.Drop + 1, want: "Call(3)"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, tt.give.String(), tt.want, "String must return the recorded spelling of the callback")
			})
		}
	})

	t.Run("ValidateKanon", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrUnknownCall for a Call that is not Valid", func(t *testing.T) {
			t.Parallel()
			prop.ErrorIs(t, func(c protocol.Call) error { return c.ValidateKanon() }, protocol.ErrUnknownCall,
				"the codec must refuse a callback outside the two",
				prop.Using(prop.Integer[protocol.Call](protocol.Drop+1, 255)))
		})

		t.Run("returns nil for a Valid Call", func(t *testing.T) {
			t.Parallel()
			prop.NoError(t, func(c protocol.Call) error { return c.ValidateKanon() },
				"the codec must accept each callback", prop.Using(prop.SampledFrom(protocol.Add, protocol.Drop)))
		})
	})
}

// TestCallAllocs checks the allocation contract of Call. MaxAllocs counts the allocations of
// the whole process, so the test does not run in parallel.
func TestCallAllocs(t *testing.T) {
	call := protocol.Drop

	t.Run("Valid", func(t *testing.T) {
		var got bool
		expect.MaxAllocs(t, func() { got = call.Valid() }, 0, "Valid must not allocate")
		assert.True(t, got, "the test must measure a Valid callback")
	})

	t.Run("String", func(t *testing.T) {
		var got string
		expect.MaxAllocs(t, func() { got = call.String() }, 0, "String of a Valid callback must not allocate")
		assert.Equal(t, got, "drop", "the test must measure the spelling of a Valid callback")
	})
}

// BenchmarkCall reports the cost of Valid, which every encode and decode of a request
// calls, and fails when it allocates.
func BenchmarkCall(b *testing.B) {
	call := protocol.Drop

	b.Run("Valid", func(b *testing.B) {
		var got bool

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			got = call.Valid()
		}

		assert.True(b, got, "the benchmark must measure a Valid callback")
	})
}
