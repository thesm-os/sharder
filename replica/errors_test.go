// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package replica_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/replica"
)

// The prefixes of the texts of the sentinel errors of package replica.
const (
	// replicaPrefix starts the text of every error that package replica declares.
	replicaPrefix = "replica: "
	// agentPrefix starts the text of every error that package replica takes from the
	// machine of the agent.
	agentPrefix = "agent: "
)

// TestErrors checks the class and the text of each sentinel error of package replica.
func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err    error
		name   string
		prefix string
		class  errs.Class
	}{
		{name: "ErrConfig", err: replica.ErrConfig, prefix: replicaPrefix, class: errs.Invalid},
		{name: "ErrNotWriter", err: replica.ErrNotWriter, prefix: agentPrefix, class: errs.Conflict},
		{name: "ErrNotServing", err: replica.ErrNotServing, prefix: agentPrefix, class: errs.Transient},
		{name: "ErrNoReplica", err: replica.ErrNoReplica, prefix: agentPrefix, class: errs.NotFound},
		{name: "ErrNoFault", err: replica.ErrNoFault, prefix: agentPrefix, class: errs.Conflict},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			t.Run("classifies as "+tt.class.String(), func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, errs.Classify(tt.err), tt.class, "the sentinel must have its class")
			})

			t.Run("starts its text with the name of the package that declares it", func(t *testing.T) {
				t.Parallel()
				assert.HasPrefix(t, tt.err.Error(), tt.prefix,
					"the text of the sentinel must start with the name of the package that declares it")
			})
		})
	}
}
