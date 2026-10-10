// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package proposal_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/proposal"
)

// errorPrefix starts the text of every error of package proposal.
const errorPrefix = "proposal: "

// TestErrors checks the class and the text of the sentinel error of package proposal.
func TestErrors(t *testing.T) {
	t.Parallel()

	t.Run("ErrUnknownKind", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies as Invalid", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, errs.Classify(proposal.ErrUnknownKind), errs.Invalid,
				"ErrUnknownKind must classify as Invalid")
		})

		t.Run("starts its text with the package's name", func(t *testing.T) {
			t.Parallel()
			assert.HasPrefix(t, proposal.ErrUnknownKind.Error(), errorPrefix,
				"the text of ErrUnknownKind must start with the name of the package")
		})
	})
}
