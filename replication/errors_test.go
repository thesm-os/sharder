// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package replication_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/replication"
)

// TestErrors checks the class of the sentinel error of package replication.
func TestErrors(t *testing.T) {
	t.Parallel()

	t.Run("ErrInvalidModel", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies as Invalid", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, errs.Classify(replication.ErrInvalidModel), errs.Invalid,
				"ErrInvalidModel must classify as Invalid")
		})
	})
}
