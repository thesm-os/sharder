// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package routestest_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/internal/machine/routes/routestest"
)

// TestErrors checks the class of the sentinel error of package routestest.
func TestErrors(t *testing.T) {
	t.Parallel()

	t.Run("ErrUnviewed", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies as Invalid", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, errs.Classify(routestest.ErrUnviewed), errs.Invalid, "ErrUnviewed must classify as Invalid")
		})
	})
}
