// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placement_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/placement"
)

// errorPrefix starts the text of every error of package placement.
const errorPrefix = "placement: "

// TestErrors checks the class and the text of each sentinel error of package
// placement.
func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		err   error
		class errs.Class
	}{
		{name: "ErrInput", err: placement.ErrInput, class: errs.Invalid},
		{name: "ErrMove", err: placement.ErrMove, class: errs.Invalid},
		{name: "ErrPlan", err: placement.ErrPlan, class: errs.Invalid},
		{name: "ErrCost", err: placement.ErrCost, class: errs.Invalid},
		{name: "ErrNoNode", err: placement.ErrNoNode, class: errs.Conflict},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			t.Run("classifies as "+tt.class.String(), func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, errs.Classify(tt.err), tt.class, "the sentinel must have its class")
			})

			t.Run("starts its text with the package's name", func(t *testing.T) {
				t.Parallel()
				assert.HasPrefix(t, tt.err.Error(), errorPrefix,
					"the text of the sentinel must start with the name of the package")
			})
		})
	}
}
