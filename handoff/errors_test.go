// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package handoff_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/handoff"
)

// TestErrors checks the class of each sentinel error of package handoff.
func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		err   error
		class errs.Class
	}{
		{name: "ErrScript", err: handoff.ErrScript, class: errs.Invalid},
		{name: "ErrTransfer", err: handoff.ErrTransfer, class: errs.Invalid},
		{name: "ErrUnknownSide", err: handoff.ErrUnknownSide, class: errs.Invalid},
		{name: "ErrUnknownEvent", err: handoff.ErrUnknownEvent, class: errs.Invalid},
		{name: "ErrUnknownAction", err: handoff.ErrUnknownAction, class: errs.Invalid},
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
