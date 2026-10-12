// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/internal/machine/agent"
)

// errorPrefix starts the text of every error of package agent.
const errorPrefix = "agent: "

// TestErrors checks the class and the text of each sentinel error of package agent.
func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err   error
		name  string
		class errs.Class
	}{
		{name: "ErrConfig", err: agent.ErrConfig, class: errs.Invalid},
		{name: "ErrNotWriter", err: agent.ErrNotWriter, class: errs.Conflict},
		{name: "ErrNotServing", err: agent.ErrNotServing, class: errs.Transient},
		{name: "ErrUnseen", err: agent.ErrUnseen, class: errs.Transient},
		{name: "ErrDenied", err: agent.ErrDenied, class: errs.Denied},
		{name: "ErrStale", err: agent.ErrStale, class: errs.Conflict},
		{name: "ErrReused", err: agent.ErrReused, class: errs.Invalid},
		{name: "ErrRequest", err: agent.ErrRequest, class: errs.Invalid},
		{name: "ErrNoReplica", err: agent.ErrNoReplica, class: errs.NotFound},
		{name: "ErrNoFault", err: agent.ErrNoFault, class: errs.Conflict},
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
