// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package root_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/internal/machine/root"
)

// errorPrefix starts the text of every error of package root.
const errorPrefix = "root: "

// TestErrors checks the class and the text of each sentinel error of package root.
func TestErrors(t *testing.T) {
	t.Parallel()

	t.Run("ErrConfig", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies as "+errs.Invalid.String(), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, errs.Classify(root.ErrConfig), errs.Invalid, "the sentinel must have its class")
		})

		t.Run("starts its text with the package's name", func(t *testing.T) {
			t.Parallel()
			assert.HasPrefix(t, root.ErrConfig.Error(), errorPrefix,
				"the text of the sentinel must start with the name of the package")
		})
	})
}
