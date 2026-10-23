// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package membershiptest_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/membership/membershiptest"
)

// TestErrors checks the class of the sentinel error of package membershiptest.
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
}
