// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard_test

import (
	"encoding/hex"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/shard"
)

// The recorded encodings of replicas.
const (
	// recordedReplicaHex pins the encoding of recordedReplica, which every placement
	// persists: field 1 is the node n1, and field 2 the role primary, 3.
	recordedReplicaHex = "0a02" + "6e31" +
		"1003"

	// unknownRoleHex is recordedReplicaHex with the role 8, which no constant has.
	unknownRoleHex = "0a02" + "6e31" +
		"1008"
)

// recordedReplica is the replica whose encoding recordedReplicaHex pins.
var recordedReplica = shard.Replica{Node: "n1", Role: shard.RolePrimary}

// TestReplica checks the recorded encoding of a Replica, and the refusal of a Role
// that is not valid.
func TestReplica(t *testing.T) {
	t.Parallel()

	t.Run("AppendBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("appends the recorded encoding of a Replica", func(t *testing.T) {
			t.Parallel()
			r := recordedReplica
			got, err := r.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode the Replica")
			assert.Equal(
				t,
				hex.EncodeToString(got),
				recordedReplicaHex,
				"AppendBinary must write fields 1 and 2 as recorded",
			)
		})

		t.Run("returns ErrUnknownRole for a Role that is not valid", func(t *testing.T) {
			t.Parallel()
			r := shard.Replica{Node: "n1", Role: shard.RoleLearner + 1}
			_, err := r.AppendBinary(nil)
			assert.ErrorIs(t, err, shard.ErrUnknownRole, "AppendBinary must refuse a role outside the eight")
		})
	})

	t.Run("UnmarshalBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the Replica of the recorded encoding", func(t *testing.T) {
			t.Parallel()
			var got shard.Replica
			assert.NoError(
				t,
				got.UnmarshalBinary(decodeHex(t, recordedReplicaHex)),
				"UnmarshalBinary must decode the recorded encoding",
			)
			assert.Equal(t, got, recordedReplica, "UnmarshalBinary must return the recorded Replica")
		})

		t.Run("returns ErrUnknownRole for an encoded Role that is not valid", func(t *testing.T) {
			t.Parallel()
			var got shard.Replica
			assert.ErrorIs(t, got.UnmarshalBinary(decodeHex(t, unknownRoleHex)), shard.ErrUnknownRole,
				"UnmarshalBinary must refuse a role outside the eight")
		})
	})
}

// TestReplicaAllocs checks the allocation contract of Replica. MaxAllocs counts the
// allocations of the whole process, so the test does not run in parallel.
func TestReplicaAllocs(t *testing.T) {
	node, role := recordedReplica.Node, recordedReplica.Role
	other := shard.Replica{Node: node, Role: shard.RoleSecondary}

	t.Run("constructs a Replica without an allocation", func(t *testing.T) {
		var got shard.Replica
		expect.MaxAllocs(
			t,
			func() { got = shard.Replica{Node: node, Role: role} },
			0,
			"constructing a Replica must not allocate",
		)
		assert.Equal(t, got, recordedReplica, "the test must measure the construction of the Replica")
	})

	t.Run("compares two Replicas without an allocation", func(t *testing.T) {
		var got bool
		expect.MaxAllocs(t, func() { got = recordedReplica == other }, 0, "comparing two Replicas must not allocate")
		assert.False(t, got, "the test must compare two Replicas that differ")
	})
}
