// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard_test

import (
	"testing"

	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/shard"
)

// TestRoleCount checks that RoleCount is the number of roles, because the arrays that a
// role indexes have RoleCount elements.
func TestRoleCount(t *testing.T) {
	t.Parallel()

	t.Run("IsValid", func(t *testing.T) {
		t.Parallel()

		t.Run("reports true for each value below RoleCount", func(t *testing.T) {
			t.Parallel()
			prop.Equal(t, shard.Role.IsValid, func(r shard.Role) bool { return int(r) < shard.RoleCount },
				"a role must be valid exactly when it indexes an array of RoleCount elements",
				prop.Using(prop.Integer[shard.Role](0, 255)))
		})
	})
}
