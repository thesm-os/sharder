// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/shard"
)

// TestRole checks Valid and String of every Role.
func TestRole(t *testing.T) {
	t.Parallel()

	t.Run("Valid", func(t *testing.T) {
		t.Parallel()

		t.Run("reports true for each of the eight roles", func(t *testing.T) {
			t.Parallel()
			prop.True(t, shard.Role.Valid, "every constant of Role must be Valid",
				prop.Using(prop.SampledFrom(shard.RoleNone, shard.RoleOffline, shard.RoleDropped, shard.RolePrimary,
					shard.RoleSecondary, shard.RoleLeader, shard.RoleFollower, shard.RoleLearner)))
		})

		t.Run("reports false for a value above RoleLearner", func(t *testing.T) {
			t.Parallel()
			prop.False(t, shard.Role.Valid, "a value above the last constant must not be Valid",
				prop.Using(prop.Integer[shard.Role](shard.RoleLearner+1, 255)))
		})
	})

	t.Run("String", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			give shard.Role
			want string
		}{
			{name: "returns none for RoleNone", give: shard.RoleNone, want: "none"},
			{name: "returns offline for RoleOffline", give: shard.RoleOffline, want: "offline"},
			{name: "returns dropped for RoleDropped", give: shard.RoleDropped, want: "dropped"},
			{name: "returns primary for RolePrimary", give: shard.RolePrimary, want: "primary"},
			{name: "returns secondary for RoleSecondary", give: shard.RoleSecondary, want: "secondary"},
			{name: "returns leader for RoleLeader", give: shard.RoleLeader, want: "leader"},
			{name: "returns follower for RoleFollower", give: shard.RoleFollower, want: "follower"},
			{name: "returns learner for RoleLearner", give: shard.RoleLearner, want: "learner"},
			{name: "returns the number of a value that is not Valid", give: shard.RoleLearner + 1, want: "Role(8)"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, tt.give.String(), tt.want, "String must return the recorded spelling of the role")
			})
		}
	})
}

// TestRoleAllocs checks the allocation contract of Role. MaxAllocs counts the
// allocations of the whole process, so the test does not run in parallel.
func TestRoleAllocs(t *testing.T) {
	role := shard.RolePrimary

	t.Run("Valid", func(t *testing.T) {
		var got bool
		expect.MaxAllocs(t, func() { got = role.Valid() }, 0, "Valid must not allocate")
		assert.True(t, got, "the test must measure a Valid role")
	})

	t.Run("String", func(t *testing.T) {
		var got string
		expect.MaxAllocs(t, func() { got = role.String() }, 0, "String of a Valid role must not allocate")
		assert.Equal(t, got, "primary", "the test must measure the spelling of a Valid role")
	})
}

// BenchmarkRole reports the cost of Valid, which every encode and decode of a role
// calls, and fails when it allocates.
func BenchmarkRole(b *testing.B) {
	role := shard.RolePrimary

	b.Run("Valid", func(b *testing.B) {
		var got bool

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			got = role.Valid()
		}

		assert.True(b, got, "the benchmark must measure a Valid role")
	})
}
