// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package control_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/internal/control"
)

// TestErrors checks the class of each sentinel error of package control.
func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		err   error
		class errs.Class
	}{
		{name: "ErrConfig", err: control.ErrConfig, class: errs.Invalid},
		{name: "ErrSpec", err: control.ErrSpec, class: errs.Invalid},
		{name: "ErrNotServing", err: control.ErrNotServing, class: errs.Transient},
		{name: "ErrDenied", err: control.ErrDenied, class: errs.Denied},
		{name: "ErrCall", err: control.ErrCall, class: errs.Invalid},
		{name: "ErrStale", err: control.ErrStale, class: errs.Conflict},
		{name: "ErrApplied", err: control.ErrApplied, class: errs.Conflict},
		{name: "ErrBusy", err: control.ErrBusy, class: errs.Transient},
		{name: "ErrBudget", err: control.ErrBudget, class: errs.Conflict},
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
