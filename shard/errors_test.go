// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/shard"
)

// TestErrors checks the class of each sentinel error of package shard.
func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		give error
		want errs.Class
	}{
		{name: "ErrIncarnation", give: shard.ErrIncarnation, want: errs.Conflict},
		{name: "ErrUnknownRole", give: shard.ErrUnknownRole, want: errs.Invalid},
		{name: "ErrUnknownStatus", give: shard.ErrUnknownStatus, want: errs.Invalid},
		{name: "ErrUnknownFault", give: shard.ErrUnknownFault, want: errs.Invalid},
		{name: "ErrUnknownPhase", give: shard.ErrUnknownPhase, want: errs.Invalid},
		{name: "ErrDuration", give: shard.ErrDuration, want: errs.Invalid},
		{name: "ErrID", give: shard.ErrID, want: errs.Invalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			t.Run("classifies as "+tt.want.String(), func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, errs.Classify(tt.give), tt.want, "the sentinel must classify as its documented class")
			})
		})
	}
}
