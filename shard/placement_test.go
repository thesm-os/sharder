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

// recordedPlacementHex pins the encoding of recordedPlacement, which every cell
// scope persists: field 1 is the shard, field 2 the range, field 3 the epoch 2, field
// 4 the reserved epoch 3, field 5 the sequence 4, field 6 one replica as an element
// with the length of its encoding, and field 7 the incarnation 5.
const recordedPlacementHex = "0a10" + recordedIDHex +
	"1206" + recordedRangeHex +
	"1802" +
	"2003" +
	"2804" +
	"3207" + "06" + recordedReplicaHex +
	"3805"

// recordedPlacement is the placement whose encoding recordedPlacementHex pins.
var recordedPlacement = shard.Placement{
	Shard:       recordedID,
	Range:       recordedRange,
	Epoch:       2,
	Incarnation: 5,
	Reserved:    3,
	Sequence:    4,
	Replicas:    []shard.Replica{recordedReplica},
}

// TestPlacement checks the recorded encoding of a Placement, and the refusal of a
// replica whose Role is not valid.
func TestPlacement(t *testing.T) {
	t.Parallel()

	t.Run("AppendBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("appends the recorded encoding of a Placement", func(t *testing.T) {
			t.Parallel()
			p := recordedPlacement
			got, err := p.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode the Placement")
			assert.Equal(
				t,
				hex.EncodeToString(got),
				recordedPlacementHex,
				"AppendBinary must write fields 1 to 7 as recorded",
			)
		})

		t.Run("returns ErrUnknownRole for a replica whose Role is not valid", func(t *testing.T) {
			t.Parallel()
			p := shard.Placement{
				Shard:    recordedID,
				Replicas: []shard.Replica{{Node: "n1", Role: shard.RoleLearner + 1}},
			}
			_, err := p.AppendBinary(nil)
			assert.ErrorIs(
				t,
				err,
				shard.ErrUnknownRole,
				"AppendBinary must refuse a replica with a role outside the eight",
			)
		})
	})

	t.Run("UnmarshalBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the Placement of the recorded encoding", func(t *testing.T) {
			t.Parallel()
			var got shard.Placement
			assert.NoError(t, got.UnmarshalBinary(decodeHex(t, recordedPlacementHex)),
				"UnmarshalBinary must decode the recorded encoding")
			assert.Equal(t, got, recordedPlacement, "UnmarshalBinary must return the recorded Placement")
		})
	})
}

// TestPlacementAllocs checks the allocation contracts of Placement and Assignment.
// MaxAllocs counts the allocations of the whole process, so the test does not run in
// parallel.
func TestPlacementAllocs(t *testing.T) {
	p := recordedPlacement
	placements := map[shard.ID]shard.Placement{recordedID: recordedPlacement}

	t.Run("constructs a Placement without an allocation", func(t *testing.T) {
		var got shard.Placement
		expect.MaxAllocs(t, func() {
			got = shard.Placement{
				Shard: p.Shard, Range: p.Range, Epoch: p.Epoch, Incarnation: p.Incarnation, Reserved: p.Reserved,
				Sequence: p.Sequence, Replicas: p.Replicas,
			}
		}, 0, "constructing a Placement must not allocate")
		assert.Equal(t, got, recordedPlacement, "the test must measure the construction of the Placement")
	})

	t.Run("Assignment", func(t *testing.T) {
		t.Run("constructs an Assignment without an allocation", func(t *testing.T) {
			var got shard.Assignment
			expect.MaxAllocs(
				t,
				func() { got = shard.Assignment{Scope: recordedScope, Generation: 1, Placements: placements} },
				0,
				"constructing an Assignment must not allocate",
			)
			assert.Equal(t, got.Scope, recordedScope, "the test must measure the construction of the Assignment")
		})

		t.Run("looks up a placement without an allocation", func(t *testing.T) {
			a := shard.Assignment{Scope: recordedScope, Generation: 1, Placements: placements}
			var got shard.Placement
			expect.MaxAllocs(
				t,
				func() { got = a.Placements[recordedID] },
				0,
				"a lookup in Placements must not allocate",
			)
			assert.Equal(t, got.Shard, recordedID, "the test must look up a shard of the assignment")
		})
	})
}
