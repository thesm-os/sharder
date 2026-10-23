// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package follow_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/internal/follow"
)

// TestErrors checks the class of the sentinel error of package follow.
func TestErrors(t *testing.T) {
	t.Parallel()

	t.Run("ErrConfig", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies as Invalid", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, errs.Classify(follow.ErrConfig), errs.Invalid,
				"the sentinel must classify as its documented class")
		})
	})
}
