// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package budget_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/budget"
)

// errorPrefix starts the text of every error of package budget.
const errorPrefix = "budget: "

// TestErrors checks the class and the text of each sentinel error of package budget.
func TestErrors(t *testing.T) {
	t.Parallel()

	t.Run("ErrUnknownImpact", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies as Invalid", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, errs.Classify(budget.ErrUnknownImpact), errs.Invalid,
				"ErrUnknownImpact must classify as Invalid")
		})

		t.Run("starts its text with the package's name", func(t *testing.T) {
			t.Parallel()
			assert.HasPrefix(t, budget.ErrUnknownImpact.Error(), errorPrefix,
				"the text of ErrUnknownImpact must start with the name of the package")
		})
	})

	t.Run("ErrReusedID", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies as Invalid", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, errs.Classify(budget.ErrReusedID), errs.Invalid, "ErrReusedID must classify as Invalid")
		})

		t.Run("starts its text with the package's name", func(t *testing.T) {
			t.Parallel()
			assert.HasPrefix(t, budget.ErrReusedID.Error(), errorPrefix,
				"the text of ErrReusedID must start with the name of the package")
		})
	})
}
