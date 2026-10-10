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

// recordedOperationHex pins the encoding of recordedOperation, which every
// transition record and every callback persists: field 1 is the incarnation, field 2
// the shard, and field 3 the sequence 7.
const recordedOperationHex = "0a10" + recordedIncarnationHex +
	"1210" + recordedIDHex +
	"1807"

// recordedOperation is the operation ID whose encoding recordedOperationHex pins.
var recordedOperation = shard.OperationID{Incarnation: recordedIncarnation, Shard: recordedID, Sequence: 7}

// TestOperationID checks the recorded encoding of an OperationID.
func TestOperationID(t *testing.T) {
	t.Parallel()

	t.Run("AppendBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("appends the recorded encoding of an OperationID", func(t *testing.T) {
			t.Parallel()
			o := recordedOperation
			got, err := o.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode the OperationID")
			assert.Equal(
				t,
				hex.EncodeToString(got),
				recordedOperationHex,
				"AppendBinary must write fields 1 to 3 as recorded",
			)
		})
	})

	t.Run("UnmarshalBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the OperationID of the recorded encoding", func(t *testing.T) {
			t.Parallel()
			var got shard.OperationID
			assert.NoError(t, got.UnmarshalBinary(decodeHex(t, recordedOperationHex)),
				"UnmarshalBinary must decode the recorded encoding")
			assert.Equal(t, got, recordedOperation, "UnmarshalBinary must return the recorded OperationID")
		})
	})
}

// TestOperationIDAllocs checks the allocation contract of OperationID. MaxAllocs
// counts the allocations of the whole process, so the test does not run in parallel.
func TestOperationIDAllocs(t *testing.T) {
	incarnation, shardID, sequence := recordedOperation.Incarnation, recordedOperation.Shard, recordedOperation.Sequence
	next := shard.OperationID{Incarnation: incarnation, Shard: shardID, Sequence: sequence + 1}
	transitions := map[shard.OperationID]int{recordedOperation: 1}

	t.Run("constructs an OperationID without an allocation", func(t *testing.T) {
		var got shard.OperationID
		expect.MaxAllocs(
			t,
			func() { got = shard.OperationID{Incarnation: incarnation, Shard: shardID, Sequence: sequence} },
			0,
			"constructing an OperationID must not allocate",
		)
		assert.Equal(t, got, recordedOperation, "the test must measure the construction of the OperationID")
	})

	t.Run("compares two OperationIDs without an allocation", func(t *testing.T) {
		var got bool
		expect.MaxAllocs(
			t,
			func() { got = recordedOperation == next },
			0,
			"comparing two OperationIDs must not allocate",
		)
		assert.False(t, got, "the test must compare two OperationIDs that differ")
	})

	t.Run("looks up a map keyed by OperationID without an allocation", func(t *testing.T) {
		var got int
		expect.MaxAllocs(t, func() { got = transitions[recordedOperation] }, 0,
			"a lookup in a map keyed by OperationID must not allocate")
		assert.Equal(t, got, 1, "the test must look up a key of the map")
	})
}
