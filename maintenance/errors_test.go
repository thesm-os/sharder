// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package maintenance_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/maintenance"
)

// TestErrors checks the class of each error of package maintenance.
func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err   error
		name  string
		class errs.Class
	}{
		{name: "ErrConfig", err: maintenance.ErrConfig, class: errs.Invalid},
		{name: "ErrNoNamespace", err: maintenance.ErrNoNamespace, class: errs.NotFound},
		{name: "ErrNoController", err: maintenance.ErrNoController, class: errs.NotFound},
		{name: "ErrNoNode", err: maintenance.ErrNoNode, class: errs.NotFound},
		{name: "ErrNoShard", err: maintenance.ErrNoShard, class: errs.NotFound},
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
