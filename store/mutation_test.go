// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package store_test

import (
	"encoding/hex"
	"testing"
	"time"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/partition/singleton"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// The recorded encodings of mutations and of the values that they contain.
const (
	// recordedPlacementHex is the encoding of the placement of recordedMutation. Field
	// 1 is the shard. Field 2 is the range from a to b. Fields 3, 4 and 5 are the epoch
	// 7, the reserved epoch 8 and the sequence 4. Field 6 lists the replica of n2 in the
	// role primary, 7 bytes long.
	recordedPlacementHex = "0a10" + recordedShardHex +
		"1206" + "0a01" + "61" + "1201" + "62" +
		"1807" +
		"2008" +
		"2804" +
		"3207" + "06" + "0a02" + "6e32" + "1003"
	// recordedProposalHex is the encoding of the proposal of recordedApplied: the scope
	// orders/c1, the incarnation, the ID, the kind move, the base 7, the body {} and the
	// expiry, one second after the Unix epoch in UTC.
	recordedProposalHex = "0a0c" + "0a06" + "6f7264657273" + "1202" + "6331" +
		"1210" + recordedIncarnationHex +
		"1a10" + recordedProposalIDHex +
		"2002" +
		"2807" +
		"3202" + "7b7d" +
		"3a02" + "0802"
	// recordedAppliedHex pins the encoding of recordedApplied. Field 1 is the proposal,
	// 62 bytes long, and field 2 the caller.
	recordedAppliedHex = "0a3e" + recordedProposalHex + "1213" + recordedCallerHex
	// recordedCellHex is the encoding of the controller record of recordedMutation:
	// the cell c1 at the epoch 2.
	recordedCellHex = "0a02" + "6331" + "2802"
	// recordedMappingHex is the encoding of the mapping of recordedMutation: the
	// partitioner singleton.
	recordedMappingHex = "0a09" + "73696e676c65746f6e"
	// recordedReservationHex is the encoding of the reservation of recordedMutation:
	// a disruption with the recorded ID alone.
	recordedReservationHex = "0a12" + "0a10" + recordedDisruptionHex

	// recordedMutationHex pins the encoding of recordedMutation. Each list is one field
	// whose elements follow each other, each with its length.
	//
	//   - Field 1 is the commit ID, 22 bytes long.
	//   - Field 2 puts the placement, 41 bytes long.
	//   - Field 3 advances the transition, 109 bytes long.
	//   - Field 4 ends the transition of the shard.
	//   - Fields 5 and 6 are the controller record and the mapping.
	//   - Field 7 is the spec {}.
	//   - Field 8 applies the proposal, 85 bytes long.
	//   - Field 9 forgets the proposal ID.
	//   - Fields 10 and 11 reserve and release the disruption.
	//   - Fields 12 and 13 add the drain and remove its key.
	//   - Field 14 confirms the commit ID.
	recordedMutationHex = "0a16" + recordedCommitIDHex +
		"122a" + "29" + recordedPlacementHex +
		"1a6e" + "6d" + recordedTransitionHex +
		"2211" + "10" + recordedShardHex +
		"2a06" + recordedCellHex +
		"320b" + recordedMappingHex +
		"3a02" + "7b7d" +
		"4256" + "55" + recordedAppliedHex +
		"4a11" + "10" + recordedProposalIDHex +
		"5215" + "14" + recordedReservationHex +
		"5a11" + "10" + recordedDisruptionHex +
		"6230" + "2f" + recordedDrainHex +
		"6a17" + "16" + recordedDrainKeyHex +
		"7217" + "16" + recordedCommitIDHex

	// recordedDigestHex is the SHA-256 digest of the bytes of recordedMutationHex.
	recordedDigestHex = "8013c6a3676f273f008d2c1f71099450933f393f116b7992d71bc484a587817c"

	// recordedSpec is the spec of recordedMutation.
	recordedSpec = "{}"
)

// The recorded applied proposal and mutation.
var (
	// recordedApplied is the applied proposal whose encoding recordedAppliedHex pins.
	recordedApplied = store.Applied{
		Proposal: proposal.Proposal{
			Scope:       shard.Scope{Namespace: recordedNamespace, Cell: recordedCell},
			Incarnation: recordedIncarnation,
			ID:          recordedProposalID,
			Kind:        proposal.Move,
			Base:        7,
			Body:        []byte(recordedSpec),
			Expires:     time.Unix(1, 0).UTC(),
		},
		Caller: recordedCaller,
	}
	// recordedMutation is the mutation whose encoding recordedMutationHex pins. It sets
	// every field.
	recordedMutation = store.Mutation{
		ID: recordedCommitID,
		Put: []shard.Placement{{
			Shard:    recordedShard,
			Range:    shard.Range{Start: []byte("a"), End: []byte("b")},
			Epoch:    7,
			Reserved: 8,
			Sequence: 4,
			Replicas: []shard.Replica{{Node: recordedTo, Role: shard.RolePrimary}},
		}},
		Transitions: []store.Transition{recordedTransition},
		Done:        []shard.ID{recordedShard},
		Cell:        &shard.Cell{ID: recordedCell, Epoch: 2},
		Mapping:     &shard.Mapping{Partitioner: singleton.Name},
		Spec:        []byte(recordedSpec),
		Applied:     []store.Applied{recordedApplied},
		Forget:      []proposal.ID{recordedProposalID},
		Reserve:     []budget.Reservation{{Disruption: budget.Disruption{ID: recordedDisruption}}},
		Release:     []budget.ID{recordedDisruption},
		Drain:       []store.Drain{recordedDrain},
		Undrain:     []store.DrainKey{recordedDrainKey},
		Confirm:     []store.CommitID{recordedCommitID},
	}
)

// TestMutation checks the recorded encodings of a Mutation and an Applied, and the
// digest of a mutation.
func TestMutation(t *testing.T) {
	t.Parallel()

	t.Run("AppendBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("appends the recorded encoding of a Mutation", func(t *testing.T) {
			t.Parallel()
			m := recordedMutation
			got, err := m.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode the Mutation")
			assert.Equal(t, hex.EncodeToString(got), recordedMutationHex,
				"AppendBinary must write fields 1 to 14 as recorded")
		})
	})

	t.Run("UnmarshalBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the Mutation of the recorded encoding", func(t *testing.T) {
			t.Parallel()
			data, err := hex.DecodeString(recordedMutationHex)
			assert.NoError(t, err, "the recorded encoding must be hexadecimal")
			var got store.Mutation
			assert.NoError(t, got.UnmarshalBinary(data), "UnmarshalBinary must decode the recorded encoding")
			assert.Equal(t, got, recordedMutation, "UnmarshalBinary must return the recorded Mutation")
		})
	})

	t.Run("Digest", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the SHA-256 digest of the recorded encoding", func(t *testing.T) {
			t.Parallel()
			m := recordedMutation
			got, err := m.Digest()
			assert.NoError(t, err, "Digest must encode the Mutation")
			assert.Equal(t, hex.EncodeToString(got.Bytes()), recordedDigestHex,
				"Digest must return the SHA-256 digest of the recorded encoding")
		})

		t.Run("returns ErrUnknownRole for a placement with a replica whose Role is not valid", func(t *testing.T) {
			t.Parallel()
			m := store.Mutation{Put: []shard.Placement{{Replicas: []shard.Replica{{Role: shard.RoleLearner + 1}}}}}
			_, err := m.Digest()
			assert.ErrorIs(t, err, shard.ErrUnknownRole, "Digest must refuse a mutation that does not encode")
		})
	})

	t.Run("Applied", func(t *testing.T) {
		t.Parallel()

		t.Run("AppendBinary", func(t *testing.T) {
			t.Parallel()

			t.Run("appends the recorded encoding of an Applied", func(t *testing.T) {
				t.Parallel()
				a := recordedApplied
				got, err := a.AppendBinary(nil)
				assert.NoError(t, err, "AppendBinary must encode the Applied")
				assert.Equal(t, hex.EncodeToString(got), recordedAppliedHex,
					"AppendBinary must write fields 1 and 2 as recorded")
			})
		})

		t.Run("UnmarshalBinary", func(t *testing.T) {
			t.Parallel()

			t.Run("returns the Applied of the recorded encoding", func(t *testing.T) {
				t.Parallel()
				data, err := hex.DecodeString(recordedAppliedHex)
				assert.NoError(t, err, "the recorded encoding must be hexadecimal")
				var got store.Applied
				assert.NoError(t, got.UnmarshalBinary(data), "UnmarshalBinary must decode the recorded encoding")
				assert.Equal(t, got, recordedApplied, "UnmarshalBinary must return the recorded Applied")
			})
		})
	})
}
