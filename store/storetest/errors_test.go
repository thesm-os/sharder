// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package storetest_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/store/storetest"
)

// TestErrors checks the classes of the sentinel errors of package storetest.
func TestErrors(t *testing.T) {
	t.Parallel()

	t.Run("ErrUnknownLoss", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies as Invalid", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, errs.Classify(storetest.ErrUnknownLoss), errs.Invalid,
				"ErrUnknownLoss must classify as Invalid")
		})
	})

	t.Run("ErrOperation", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies as Unsupported", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, errs.Classify(storetest.ErrOperation), errs.Unsupported,
				"ErrOperation must classify as Unsupported")
		})
	})
}
