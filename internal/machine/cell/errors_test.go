// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package cell_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/internal/machine/cell"
)

// TestErrors checks the class of each sentinel error of package cell.
func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		err   error
		class errs.Class
	}{
		{name: "ErrConfig", err: cell.ErrConfig, class: errs.Invalid},
		{name: "ErrRoot", err: cell.ErrRoot, class: errs.Conflict},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			t.Run("classifies as "+tt.class.String(), func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, errs.Classify(tt.err), tt.class, "the sentinel must have its class")
			})
		})
	}
}
