// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package memory_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/store/memory"
)

// TestErrors checks the class of each sentinel error of package memory.
func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		give error
		want errs.Class
	}{
		{name: "ErrOutage", give: memory.ErrOutage, want: errs.Transient},
		{name: "ErrFault", give: memory.ErrFault, want: errs.Transient},
		{name: "ErrClosed", give: memory.ErrClosed, want: errs.Invalid},
		{name: "ErrToken", give: memory.ErrToken, want: errs.Invalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			t.Run("classifies as "+tt.want.String(), func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, errs.Classify(tt.give), tt.want, "the sentinel must classify as its documented class")
			})
		})
	}
}
