// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package engine_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/internal/engine"
)

// errorPrefix starts the text of every error of package engine.
const errorPrefix = "engine: "

// TestErrors checks the class and the text of each sentinel error of package engine.
func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		err   error
		class errs.Class
	}{
		{name: "ErrConfig", err: engine.ErrConfig, class: errs.Invalid},
		{name: "ErrMove", err: engine.ErrMove, class: errs.Invalid},
		{name: "ErrRecord", err: engine.ErrRecord, class: errs.Invalid},
		{name: "ErrProtocol", err: engine.ErrProtocol, class: errs.Unsupported},
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
