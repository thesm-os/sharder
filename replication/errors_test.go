// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package replication_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/replication"
)

// errorPrefix starts the text of every error of package replication.
const errorPrefix = "replication: "

// TestErrors checks the class and the text of the sentinel error of package
// replication.
func TestErrors(t *testing.T) {
	t.Parallel()

	t.Run("ErrInvalidModel", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies as Invalid", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, errs.Classify(replication.ErrInvalidModel), errs.Invalid,
				"ErrInvalidModel must classify as Invalid")
		})

		t.Run("starts its text with the package's name", func(t *testing.T) {
			t.Parallel()
			assert.HasPrefix(t, replication.ErrInvalidModel.Error(), errorPrefix,
				"the text of ErrInvalidModel must start with the name of the package")
		})
	})
}
