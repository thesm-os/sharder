// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard_test

import (
	"encoding/json"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/shard"
)

const (
	// recordedPrimary is the spelling of RolePrimary, which the specs of namespaces
	// record.
	recordedPrimary = "primary"
	// unknownRole is a spelling of no role.
	unknownRole = "voter"
)

// TestRole checks Valid, String and the text methods of every Role.
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

		t.Run("reports true for each value below RoleCount", func(t *testing.T) {
			t.Parallel()
			prop.Equal(t, shard.Role.Valid, func(r shard.Role) bool { return int(r) < shard.RoleCount },
				"a role must be Valid exactly when it indexes an array of RoleCount elements",
				prop.Using(prop.Integer[shard.Role](0, 255)))
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

	t.Run("MarshalText", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the spelling of the role", func(t *testing.T) {
			t.Parallel()
			got, err := shard.RolePrimary.MarshalText()
			assert.NoError(t, err, "MarshalText must encode a Valid role")
			assert.Equal(t, string(got), recordedPrimary, "MarshalText must return the spelling")
		})

		t.Run("returns ErrUnknownRole for a value that is not Valid", func(t *testing.T) {
			t.Parallel()
			_, err := (shard.RoleLearner + 1).MarshalText()
			assert.ErrorIs(t, err, shard.ErrUnknownRole, "MarshalText must refuse a role that is not Valid")
		})

		t.Run("encodes a role in JSON as a string", func(t *testing.T) {
			t.Parallel()
			got, err := json.Marshal(shard.RolePrimary)
			assert.NoError(t, err, "encoding/json must encode a Valid role")
			assert.Equal(t, string(got), `"`+recordedPrimary+`"`, "encoding/json must write the spelling")
		})
	})

	t.Run("UnmarshalText", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the role that MarshalText encodes", func(t *testing.T) {
			t.Parallel()
			prop.RoundTrip(
				t,
				shard.Role.MarshalText,
				func(text []byte) (shard.Role, error) {
					var r shard.Role
					err := r.UnmarshalText(text)
					return r, err
				},
				"UnmarshalText must return the role that MarshalText encodes",
				prop.Using(prop.SampledFrom(shard.RoleNone, shard.RoleOffline, shard.RoleDropped, shard.RolePrimary,
					shard.RoleSecondary, shard.RoleLeader, shard.RoleFollower, shard.RoleLearner)),
			)
		})

		t.Run("returns ErrUnknownRole for the spelling of no role", func(t *testing.T) {
			t.Parallel()
			got := shard.RolePrimary
			err := got.UnmarshalText([]byte(unknownRole))
			expect.ErrorIs(t, err, shard.ErrUnknownRole, "UnmarshalText must return ErrUnknownRole")
			expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of UnmarshalText must classify as Invalid")
			expect.Equal(t, got, shard.RolePrimary, "UnmarshalText must leave the role unchanged")
		})

		t.Run("decodes a JSON string", func(t *testing.T) {
			t.Parallel()
			var got shard.Role
			assert.NoError(t, json.Unmarshal([]byte(`"`+recordedPrimary+`"`), &got),
				"encoding/json must decode the spelling of a role")
			assert.Equal(t, got, shard.RolePrimary, "encoding/json must return the role of the spelling")
		})
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
