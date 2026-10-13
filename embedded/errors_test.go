// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package embedded_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/embedded"
)

// embeddedPrefix starts the text of every error that package embedded declares.
const embeddedPrefix = "embedded: "

// TestErrors checks the class and the text of each error of package embedded.
func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err   error
		name  string
		class errs.Class
	}{
		{name: "ErrConfig", err: embedded.ErrConfig, class: errs.Invalid},
		{name: "ErrStarted", err: embedded.ErrStarted, class: errs.Invalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			t.Run("classifies as "+tt.class.String(), func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, errs.Classify(tt.err), tt.class, "the sentinel must have its class")
			})

			t.Run("starts its text with the name of the package", func(t *testing.T) {
				t.Parallel()
				assert.HasPrefix(t, tt.err.Error(), embeddedPrefix,
					"the text of the sentinel must start with the name of the package")
			})
		})
	}
}
