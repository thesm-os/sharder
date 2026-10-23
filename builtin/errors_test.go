// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/builtin"
)

// TestErrors checks the class of the sentinel error of package builtin.
func TestErrors(t *testing.T) {
	t.Parallel()

	t.Run("ErrConfig", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies as Invalid", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, errs.Classify(builtin.ErrConfig), errs.Invalid, "ErrConfig must classify as Invalid")
		})
	})
}
