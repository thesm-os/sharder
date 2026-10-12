// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package scope_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/internal/scope"
)

// errorPrefix is the prefix of the text of every error of package scope.
const errorPrefix = "scope: "

// TestErrors checks the class and the text of each sentinel error of package scope.
func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		give error
		want errs.Class
	}{
		{name: "ErrConfig", give: scope.ErrConfig, want: errs.Invalid},
		{name: "ErrNotAcquired", give: scope.ErrNotAcquired, want: errs.Invalid},
		{name: "ErrSchema", give: scope.ErrSchema, want: errs.Unsupported},
		{name: "ErrChanged", give: scope.ErrChanged, want: errs.Conflict},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			t.Run("classifies as "+tt.want.String(), func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, errs.Classify(tt.give), tt.want, "the sentinel must classify as its documented class")
			})

			t.Run("starts its text with the package's name", func(t *testing.T) {
				t.Parallel()
				assert.HasPrefix(t, tt.give.Error(), errorPrefix,
					"the text of the sentinel must start with the name of the package")
			})
		})
	}
}
