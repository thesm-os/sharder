// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package controller_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/controller"
)

// TestErrors checks the class of each sentinel error of package controller.
func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err   error
		name  string
		class errs.Class
	}{
		{name: "ErrConfig", err: controller.ErrConfig, class: errs.Invalid},
		{name: "ErrRun", err: controller.ErrRun, class: errs.Invalid},
		{name: "ErrScope", err: controller.ErrScope, class: errs.Invalid},
		{name: "ErrNotServing", err: controller.ErrNotServing, class: errs.Transient},
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
