// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package store_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/store"
)

// errorPrefix is the prefix of the text of every error of package store.
const errorPrefix = "store: "

// TestErrors checks the class and the text of each sentinel error of package store.
func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		give error
		want errs.Class
	}{
		{name: "ErrCompacted", give: store.ErrCompacted, want: errs.Conflict},
		{name: "ErrReused", give: store.ErrReused, want: errs.Invalid},
		{name: "ErrTooLarge", give: store.ErrTooLarge, want: errs.Invalid},
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
