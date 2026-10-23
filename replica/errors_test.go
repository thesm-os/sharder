// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package replica_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/replica"
)

// TestErrors checks the class of each sentinel error of package replica.
func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err   error
		name  string
		class errs.Class
	}{
		{name: "ErrConfig", err: replica.ErrConfig, class: errs.Invalid},
		{name: "ErrStarted", err: replica.ErrStarted, class: errs.Invalid},
		{name: "ErrNotWriter", err: replica.ErrNotWriter, class: errs.Conflict},
		{name: "ErrNotServing", err: replica.ErrNotServing, class: errs.Transient},
		{name: "ErrNoReplica", err: replica.ErrNoReplica, class: errs.NotFound},
		{name: "ErrNoFault", err: replica.ErrNoFault, class: errs.Conflict},
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
