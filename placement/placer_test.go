// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placement_test

import (
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/placement"
)

// TestPlacer checks the zero values of a Schedule, a Plan and a Target.
func TestPlacer(t *testing.T) {
	t.Parallel()

	t.Run("Schedule", func(t *testing.T) {
		t.Parallel()

		t.Run("runs no periodic pass when it is zero", func(t *testing.T) {
			t.Parallel()
			var s placement.Schedule
			assert.Equal(t, s.Interval, time.Duration(0), "the zero Schedule must have no interval")
		})

		t.Run("ignores the membership when it is zero", func(t *testing.T) {
			t.Parallel()
			var s placement.Schedule
			assert.False(t, s.OnMembership, "the zero Schedule must not run a pass for a change of the membership")
		})
	})

	t.Run("Plan", func(t *testing.T) {
		t.Parallel()

		t.Run("changes no shard when it is zero", func(t *testing.T) {
			t.Parallel()
			violations, err := placement.Validate(cell(), placement.Plan{}, nil)
			expect.NoError(t, err, "Validate must accept the zero Plan")
			expect.Empty(t, violations, "the zero Plan must have no violation without constraints")
		})
	})

	t.Run("Target", func(t *testing.T) {
		t.Parallel()

		t.Run("places no replica when it is zero", func(t *testing.T) {
			t.Parallel()
			var target placement.Target
			assert.Empty(t, target.Replicas, "the zero Target must place no replica")
		})

		t.Run("has no deficit when it is zero", func(t *testing.T) {
			t.Parallel()
			var target placement.Target
			assert.Equal(t, target.Deficit, 0, "the zero Target must have no deficit")
		})

		t.Run("has no reason when it is zero", func(t *testing.T) {
			t.Parallel()
			var target placement.Target
			assert.Nil(t, target.Reason, "the zero Target must have no reason")
		})
	})
}
