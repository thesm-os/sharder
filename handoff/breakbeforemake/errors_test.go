// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package breakbeforemake_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/handoff/breakbeforemake"
)

// errorPrefix starts the text of every error of package breakbeforemake.
const errorPrefix = "breakbeforemake: "

// TestErrors checks the class and the text of the sentinel error of package
// breakbeforemake.
func TestErrors(t *testing.T) {
	t.Parallel()

	t.Run("ErrConfig", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies as Invalid", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, errs.Classify(breakbeforemake.ErrConfig), errs.Invalid,
				"ErrConfig must classify as Invalid")
		})

		t.Run("starts its text with the package's name", func(t *testing.T) {
			t.Parallel()
			assert.HasPrefix(t, breakbeforemake.ErrConfig.Error(), errorPrefix,
				"the text of ErrConfig must start with the name of the package")
		})
	})
}
