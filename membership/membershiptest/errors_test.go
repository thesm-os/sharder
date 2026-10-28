// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package membershiptest_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/membership/membershiptest"
)

// TestErrors checks the classes of the sentinel errors of package membershiptest.
func TestErrors(t *testing.T) {
	t.Parallel()

	t.Run("ErrUnknownLoss", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies as Invalid", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, errs.Classify(membershiptest.ErrUnknownLoss), errs.Invalid,
				"ErrUnknownLoss must classify as Invalid")
		})
	})

	t.Run("ErrOperation", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies as Unsupported", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, errs.Classify(membershiptest.ErrOperation), errs.Unsupported,
				"ErrOperation must classify as Unsupported")
		})
	})
}
