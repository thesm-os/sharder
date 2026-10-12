// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package follow_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/internal/follow"
)

// errorPrefix is the prefix of the text of every error of package follow.
const errorPrefix = "follow: "

// TestErrors checks the class and the text of each sentinel error of package follow.
func TestErrors(t *testing.T) {
	t.Parallel()

	t.Run("ErrConfig", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies as Invalid", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, errs.Classify(follow.ErrConfig), errs.Invalid,
				"the sentinel must classify as its documented class")
		})

		t.Run("starts its text with the package's name", func(t *testing.T) {
			t.Parallel()
			assert.HasPrefix(t, follow.ErrConfig.Error(), errorPrefix,
				"the text of the sentinel must start with the name of the package")
		})
	})
}
