// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package constraint_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/placement/constraint"
)

// errorPrefix starts the text of every error of package constraint.
const errorPrefix = "constraint: "

// TestErrors checks the class and the text of each sentinel error of package
// constraint.
func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		err   error
		class errs.Class
	}{
		{name: "ErrConfig", err: constraint.ErrConfig, class: errs.Invalid},
		{name: "ErrCapacity", err: constraint.ErrCapacity, class: errs.Conflict},
		{name: "ErrLabels", err: constraint.ErrLabels, class: errs.Conflict},
		{name: "ErrIsolation", err: constraint.ErrIsolation, class: errs.Conflict},
		{name: "ErrDrain", err: constraint.ErrDrain, class: errs.Conflict},
		{name: "ErrMaxPerNode", err: constraint.ErrMaxPerNode, class: errs.Conflict},
		{name: "ErrMatchLabel", err: constraint.ErrMatchLabel, class: errs.Conflict},
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
