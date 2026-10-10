// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package partition_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/partition"
)

// errorPrefix starts the text of every error of package partition.
const errorPrefix = "partition: "

// TestErrors checks the class and the text of the sentinel error of package
// partition.
func TestErrors(t *testing.T) {
	t.Parallel()

	t.Run("ErrRange", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies as Invalid", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, errs.Classify(partition.ErrRange), errs.Invalid, "ErrRange must classify as Invalid")
		})

		t.Run("starts its text with the package's name", func(t *testing.T) {
			t.Parallel()
			assert.HasPrefix(t, partition.ErrRange.Error(), errorPrefix,
				"the text of ErrRange must start with the name of the package")
		})
	})
}
