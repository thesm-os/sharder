// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package routes_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/internal/machine/routes"
)

// errorPrefix starts the text of every error of package routes.
const errorPrefix = "routes: "

// TestErrors checks the class and the text of each sentinel error of package routes.
func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		give error
		name string
		want errs.Class
	}{
		{name: "ErrConfig", give: routes.ErrConfig, want: errs.Invalid},
		{name: "ErrClosed", give: routes.ErrClosed, want: errs.Invalid},
		{name: "ErrMapping", give: routes.ErrMapping, want: errs.Unsupported},
		{name: "ErrNoRoute", give: routes.ErrNoRoute, want: errs.Transient},
		{name: "ErrCell", give: routes.ErrCell, want: errs.NotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			t.Run("classifies as its class", func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, errs.Classify(tt.give), tt.want, "the error must have its class")
			})

			t.Run("starts its text with the name of the package", func(t *testing.T) {
				t.Parallel()
				assert.HasPrefix(t, tt.give.Error(), errorPrefix, "the text must start with the name of the package")
			})
		})
	}
}
