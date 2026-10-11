// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placement_test

import (
	"testing"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/placement"
)

// TestInput checks the zero values of an Input and of its parts.
func TestInput(t *testing.T) {
	t.Parallel()

	t.Run("Input", func(t *testing.T) {
		t.Parallel()

		t.Run("is not valid when it is zero", func(t *testing.T) {
			t.Parallel()
			_, err := placement.NewState(&placement.Input{})
			assert.ErrorIs(t, err, placement.ErrInput, "NewState must refuse the zero Input, whose model has no name")
		})
	})

	t.Run("InFlight", func(t *testing.T) {
		t.Parallel()

		t.Run("names no node when it is zero", func(t *testing.T) {
			t.Parallel()
			var f placement.InFlight
			assert.Empty(t, f.To, "the zero InFlight must have no destination")
			assert.Empty(t, f.Fallback, "the zero InFlight must have no fallback")
		})
	})

	t.Run("Metric", func(t *testing.T) {
		t.Parallel()

		t.Run("fills the capacity of its name when Capacity is empty", func(t *testing.T) {
			t.Parallel()
			in := cell()
			in.Metrics = []placement.Metric{{Name: mem}}
			assert.Equal(t, newState(t, in).Metrics(), []placement.Metric{{Name: mem, Capacity: mem}},
				"a metric without a capacity must fill the capacity of its name")
		})
	})
}
