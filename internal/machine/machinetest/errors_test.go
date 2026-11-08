// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machinetest_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/internal/machine/machinetest"
)

// TestErrors checks the class of each sentinel error of machinetest.
func TestErrors(t *testing.T) {
	t.Parallel()

	t.Run("ErrEnded", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies as Invalid", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, errs.Classify(machinetest.ErrEnded), errs.Invalid,
				"a write of an ended tenure never succeeds")
		})
	})

	t.Run("ErrUnknownRecordKind", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies as Invalid", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, errs.Classify(machinetest.ErrUnknownRecordKind), errs.Invalid,
				"a text that names no kind of a record never parses")
		})
	})
}
