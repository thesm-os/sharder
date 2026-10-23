// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package budget_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/budget"
)

// TestErrors checks the class of each sentinel error of package budget.
func TestErrors(t *testing.T) {
	t.Parallel()

	t.Run("ErrUnknownImpact", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies as Invalid", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, errs.Classify(budget.ErrUnknownImpact), errs.Invalid,
				"ErrUnknownImpact must classify as Invalid")
		})
	})

	t.Run("ErrReusedID", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies as Invalid", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, errs.Classify(budget.ErrReusedID), errs.Invalid, "ErrReusedID must classify as Invalid")
		})
	})
}
