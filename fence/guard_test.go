// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package fence_test

import (
	"math"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"
	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/fence"
)

// recordedRetired is the value of Retired, which operators write into the fence
// records of the application.
const recordedRetired = math.MaxInt64

// TestGuard checks the watermark Retired under core's fence laws.
func TestGuard(t *testing.T) {
	t.Parallel()

	t.Run("Retired", func(t *testing.T) {
		t.Parallel()

		t.Run("is the recorded watermark", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, fence.Retired, recordedRetired, "Retired must be the watermark that operators record")
		})

		t.Run("refuses every fenced epoch below it", func(t *testing.T) {
			t.Parallel()
			prop.False(t, func(e epoch.Epoch) bool { return epoch.Admissible(fence.Retired, e) },
				"a fence at Retired must refuse every fenced epoch below it",
				prop.Using(prop.Integer[epoch.Epoch](1, fence.Retired-1)))
		})

		t.Run("admits an unfenced write at epoch.Zero", func(t *testing.T) {
			t.Parallel()
			assert.True(t, epoch.Admissible(fence.Retired, epoch.Zero),
				"a fence at Retired must admit a write at epoch.Zero")
		})
	})
}
