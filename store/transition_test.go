// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package store_test

import (
	"encoding/hex"
	"testing"
	"time"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// The recorded encodings of transitions.
const (
	// recordedFromHex pins the encoding of recordedFromEndpoint. Field 1 is the node
	// n1, field 2 the incarnation 2, and field 3 the role primary, 3.
	recordedFromHex = "0a02" + "6e31" + "1002" + "1803"
	// recordedToHex pins the encoding of the endpoint of the node n2 at the
	// incarnation 1 in the role primary.
	recordedToHex = "0a02" + "6e32" + "1001" + "1803"
	// recordedOperationHex is the encoding of the operation of recordedTransition: the
	// incarnation, the shard and the sequence 4.
	recordedOperationHex = "0a10" + recordedIncarnationHex + "1210" + recordedShardHex + "1804"
	// recordedProtocolHex is the encoding of the name break-before-make.
	recordedProtocolHex = "627265616b2d6265666f72652d6d616b65"

	// recordedTransitionHex pins the encoding of recordedTransition, which the scope of
	// a cell keeps while the move runs. Field 1 is the operation, 38 bytes long. Field 2
	// is the protocol and field 3 its version 1. Field 4 is the spec version 2. Fields
	// 5, 6 and 7 are the endpoints from, to and fallback. Fields 8, 9 and 10 are the old
	// epoch 6, the new epoch 7 and the abort epoch 8. Field 11 is the phase aborting, 2.
	// Fields 12 and 13 are the step 1 and the failed step 3, zigzag. Field 14 is the
	// start, nine seconds after the Unix epoch in UTC.
	recordedTransitionHex = "0a26" + recordedOperationHex +
		"1211" + recordedProtocolHex +
		"1801" +
		"2002" +
		"2a08" + recordedFromHex +
		"3208" + recordedToHex +
		"3a08" + recordedFromHex +
		"4006" +
		"4807" +
		"5008" +
		"5802" +
		"6002" +
		"6806" +
		"7202" + "0812"

	// unknownPhaseHex is field 11 with the phase 5, which no constant has.
	unknownPhaseHex = "5805"

	// recordedProtocol is the protocol of recordedTransition.
	recordedProtocol shard.StrategyName = "break-before-make"
)

// The recorded endpoints and transition.
var (
	// recordedFromEndpoint is the endpoint whose encoding recordedFromHex pins.
	recordedFromEndpoint = store.Endpoint{Node: recordedFrom, Incarnation: 2, Role: shard.RolePrimary}
	// recordedTransition is the transition whose encoding recordedTransitionHex pins:
	// a move of the writer from n1 to n2 that runs the abort path of its step 3.
	recordedTransition = store.Transition{
		Operation:       shard.OperationID{Incarnation: recordedIncarnation, Shard: recordedShard, Sequence: 4},
		Protocol:        recordedProtocol,
		ProtocolVersion: 1,
		SpecVersion:     2,
		From:            recordedFromEndpoint,
		To:              store.Endpoint{Node: recordedTo, Incarnation: 1, Role: shard.RolePrimary},
		Fallback:        recordedFromEndpoint,
		OldEpoch:        6,
		NewEpoch:        7,
		AbortEpoch:      8,
		Phase:           shard.PhaseAborting,
		Step:            1,
		Failed:          3,
		Started:         time.Unix(9, 0).UTC(),
	}
)

// TestTransition checks the recorded encodings of a Transition and an Endpoint, and the
// refusal of a phase or a role that is not valid.
func TestTransition(t *testing.T) {
	t.Parallel()

	t.Run("AppendBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("appends the recorded encoding of a Transition", func(t *testing.T) {
			t.Parallel()
			tr := recordedTransition
			got, err := tr.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode the Transition")
			assert.Equal(t, hex.EncodeToString(got), recordedTransitionHex,
				"AppendBinary must write fields 1 to 14 as recorded")
		})

		t.Run("returns ErrUnknownPhase for a Phase that is not valid", func(t *testing.T) {
			t.Parallel()
			tr := store.Transition{Phase: shard.PhaseAborted + 1}
			_, err := tr.AppendBinary(nil)
			assert.ErrorIs(t, err, shard.ErrUnknownPhase, "AppendBinary must refuse a phase outside the enumeration")
		})

		t.Run("returns ErrUnknownRole for an endpoint whose Role is not valid", func(t *testing.T) {
			t.Parallel()
			tr := store.Transition{To: store.Endpoint{Role: shard.RoleLearner + 1}}
			_, err := tr.AppendBinary(nil)
			assert.ErrorIs(t, err, shard.ErrUnknownRole, "AppendBinary must refuse a role outside the enumeration")
		})
	})

	t.Run("UnmarshalBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the Transition of the recorded encoding", func(t *testing.T) {
			t.Parallel()
			data, err := hex.DecodeString(recordedTransitionHex)
			assert.NoError(t, err, "the recorded encoding must be hexadecimal")
			var got store.Transition
			assert.NoError(t, got.UnmarshalBinary(data), "UnmarshalBinary must decode the recorded encoding")
			assert.Equal(t, got, recordedTransition, "UnmarshalBinary must return the recorded Transition")
		})

		t.Run("returns ErrUnknownPhase for an encoded Phase that is not valid", func(t *testing.T) {
			t.Parallel()
			data, err := hex.DecodeString(unknownPhaseHex)
			assert.NoError(t, err, "the encoding must be hexadecimal")
			var got store.Transition
			assert.ErrorIs(t, got.UnmarshalBinary(data), shard.ErrUnknownPhase,
				"UnmarshalBinary must refuse a phase outside the enumeration")
		})
	})

	t.Run("Endpoint", func(t *testing.T) {
		t.Parallel()

		t.Run("AppendBinary", func(t *testing.T) {
			t.Parallel()

			t.Run("appends the recorded encoding of an Endpoint", func(t *testing.T) {
				t.Parallel()
				e := recordedFromEndpoint
				got, err := e.AppendBinary(nil)
				assert.NoError(t, err, "AppendBinary must encode the Endpoint")
				assert.Equal(
					t,
					hex.EncodeToString(got),
					recordedFromHex,
					"AppendBinary must write fields 1 to 3 as recorded",
				)
			})
		})

		t.Run("UnmarshalBinary", func(t *testing.T) {
			t.Parallel()

			t.Run("returns the Endpoint of the recorded encoding", func(t *testing.T) {
				t.Parallel()
				data, err := hex.DecodeString(recordedFromHex)
				assert.NoError(t, err, "the recorded encoding must be hexadecimal")
				var got store.Endpoint
				assert.NoError(t, got.UnmarshalBinary(data), "UnmarshalBinary must decode the recorded encoding")
				assert.Equal(t, got, recordedFromEndpoint, "UnmarshalBinary must return the recorded Endpoint")
			})
		})
	})
}
