// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package proposal_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/proposal"
)

// TestErrors checks the class of the sentinel error of package proposal.
func TestErrors(t *testing.T) {
	t.Parallel()

	t.Run("ErrUnknownOperationKind", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies as Invalid", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, errs.Classify(proposal.ErrUnknownOperationKind), errs.Invalid,
				"ErrUnknownOperationKind must classify as Invalid")
		})
	})
}
