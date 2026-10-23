// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package ownership_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/ownership"
)

// TestErrors checks the class of each sentinel error of package ownership.
func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		give error
		want errs.Class
	}{
		{name: "ErrConfig", give: ownership.ErrConfig, want: errs.Invalid},
		{name: "ErrUnknownOnLeaseLoss", give: ownership.ErrUnknownOnLeaseLoss, want: errs.Invalid},
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
