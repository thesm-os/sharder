// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placementtest_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/placement/placementtest"
)

// TestErrors checks the class of the sentinel error of package placementtest.
func TestErrors(t *testing.T) {
	t.Parallel()

	t.Run("ErrUnconstrained", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies as Invalid", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, errs.Classify(placementtest.ErrUnconstrained), errs.Invalid,
				"ErrUnconstrained must classify as Invalid")
		})
	})
}
