// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machine_test

import (
	"testing"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/internal/machine"
)

// member is one type of the vocabulary, as a value of the type and the name of the type.
type member struct {
	give any
	name string
}

// TestMachine checks that the first and the last event of a run are events and not
// effects.
func TestMachine(t *testing.T) {
	t.Parallel()

	events(t, []member{
		{name: "Started", give: machine.Started{}},
		{name: "Stopped", give: machine.Stopped{}},
	})
}

// TestMachineAllocs checks the allocations of an event and of an effect that a step passes
// in a slice of interface values.
func TestMachineAllocs(t *testing.T) {
	t.Run("Stopped", func(t *testing.T) {
		t.Run("does not allocate in a slice of events", func(t *testing.T) {
			evs := make([]machine.Event, 0, 1)
			assert.MaxAllocs(t, func() { evs = append(evs[:0], machine.Stopped{}) }, 0,
				"a Stopped in a slice of events must not allocate")
		})
	})

	t.Run("Commit", func(t *testing.T) {
		t.Run("allocates once in a slice of effects", func(t *testing.T) {
			out := make([]machine.Effect, 0, 1)
			var tok machine.Token
			assert.MaxAllocs(t, func() {
				tok++
				out = append(out[:0], machine.Commit{Token: tok})
			}, 1, "a Commit in a slice of effects must allocate at most once")
		})
	})
}

// effects runs the cases of each type of members, which implements Effect and does not
// implement Event.
func effects(t *testing.T, members []member) {
	t.Helper()

	for _, m := range members {
		t.Run(m.name, func(t *testing.T) {
			t.Parallel()

			t.Run("is an Effect", func(t *testing.T) {
				t.Parallel()
				_, ok := m.give.(machine.Effect)
				assert.True(t, ok, "the type must implement Effect")
			})

			t.Run("is not an Event", func(t *testing.T) {
				t.Parallel()
				_, ok := m.give.(machine.Event)
				assert.False(t, ok, "an effect must not implement Event")
			})
		})
	}
}

// events runs the cases of each type of members, which implements Event and does not
// implement Effect.
func events(t *testing.T, members []member) {
	t.Helper()

	for _, m := range members {
		t.Run(m.name, func(t *testing.T) {
			t.Parallel()

			t.Run("is an Event", func(t *testing.T) {
				t.Parallel()
				_, ok := m.give.(machine.Event)
				assert.True(t, ok, "the type must implement Event")
			})

			t.Run("is not an Effect", func(t *testing.T) {
				t.Parallel()
				_, ok := m.give.(machine.Effect)
				assert.False(t, ok, "an event must not implement Effect")
			})
		})
	}
}
