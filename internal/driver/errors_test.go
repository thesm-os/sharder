// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package driver_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/internal/driver"
)

// TestErrors checks the class of each sentinel error of package driver.
func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		give error
		name string
		want errs.Class
	}{
		{name: "ErrConfig", give: driver.ErrConfig, want: errs.Invalid},
		{name: "ErrRun", give: driver.ErrRun, want: errs.Invalid},
		{name: "ErrStopped", give: driver.ErrStopped, want: errs.Invalid},
		{name: "ErrReply", give: driver.ErrReply, want: errs.Invalid},
		{name: "ErrEffect", give: driver.ErrEffect, want: errs.Invalid},
		{name: "ErrEnded", give: driver.ErrEnded, want: errs.Transient},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			t.Run("classifies as its class", func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, errs.Classify(tt.give), tt.want, "the error must have its class")
			})
		})
	}
}
