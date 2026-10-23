// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package root_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/internal/machine/root"
)

// TestErrors checks the class of the sentinel error of package root.
func TestErrors(t *testing.T) {
	t.Parallel()

	t.Run("ErrConfig", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies as "+errs.Invalid.String(), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, errs.Classify(root.ErrConfig), errs.Invalid, "the sentinel must have its class")
		})
	})
}
