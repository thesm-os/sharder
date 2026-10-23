// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package budget_test

import (
	"encoding/hex"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/crypto"
	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/shard"
)

// The recorded encodings of reservations.
const (
	// recordedDigestHex is the SHA-256 digest of the bytes of recordedDisruptionHex.
	recordedDigestHex = "36aa26c8f4fe3508c1e60297a4fc2b12c7fbc59dcf32a5fcea404b0ecf65a989"
	// recordedOwnerHex is the encoding of recordedOwner: its UTF-8 bytes.
	recordedOwnerHex = "7370696666653a2f2f6f72646572732f6e31"

	// recordedReservationHex pins the encoding of recordedReservation, which the scope
	// of its cell keeps. Field 1 is the encoding of recordedDisruption, 69 bytes long.
	// Field 2 is the digest, and field 3 the owner. Field 4 contains the incarnation 3
	// of the node n1. Field 5 is the admission, three seconds after the Unix epoch in
	// UTC.
	recordedReservationHex = "0a45" + recordedDisruptionHex +
		"1220" + recordedDigestHex +
		"1a12" + recordedOwnerHex +
		"2204" + "02" + "6e31" + "03" +
		"2a02" + "0806"

	// unknownNestedImpactHex is field 1 with a Disruption whose field 6 is the impact
	// 4, which no constant has.
	unknownNestedImpactHex = "0a02" + unknownImpactHex

	// recordedOwner is the identity of the caller that requested the disruption of
	// recordedReservation.
	recordedOwner shard.Identity = "spiffe://orders/n1"
)

// recordedReservation is the reservation whose encoding recordedReservationHex pins: the
// admitted restart of the node n1.
var recordedReservation = budget.Reservation{
	Disruption: recordedDisruption,
	Digest: crypto.NewDigest256([crypto.DigestSize256]byte{
		0x36, 0xaa, 0x26, 0xc8, 0xf4, 0xfe, 0x35, 0x08,
		0xc1, 0xe6, 0x02, 0x97, 0xa4, 0xfc, 0x2b, 0x12,
		0xc7, 0xfb, 0xc5, 0x9d, 0xcf, 0x32, 0xa5, 0xfc,
		0xea, 0x40, 0x4b, 0x0e, 0xcf, 0x65, 0xa9, 0x89,
	}),
	Owner:        recordedOwner,
	Incarnations: map[shard.NodeID]epoch.Epoch{recordedNode: recordedNodeIncarnation},
	Admitted:     time.Unix(3, 0).UTC(),
}

// TestReservation checks the recorded encoding of a Reservation, and the refusal of a
// Disruption whose impact is not valid.
func TestReservation(t *testing.T) {
	t.Parallel()

	t.Run("AppendBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("appends the recorded encoding of a Reservation", func(t *testing.T) {
			t.Parallel()
			r := recordedReservation
			got, err := r.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode the Reservation")
			assert.Equal(t, hex.EncodeToString(got), recordedReservationHex,
				"AppendBinary must write fields 1 to 5 as recorded")
		})

		t.Run("returns ErrUnknownImpact for a Disruption whose Impact is not valid", func(t *testing.T) {
			t.Parallel()
			r := budget.Reservation{Disruption: budget.Disruption{Impact: budget.FullLoss + 1}}
			_, err := r.AppendBinary(nil)
			assert.ErrorIs(t, err, budget.ErrUnknownImpact,
				"AppendBinary must refuse a disruption with an impact outside the enumeration")
		})
	})

	t.Run("UnmarshalBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the Reservation of the recorded encoding", func(t *testing.T) {
			t.Parallel()
			data, err := hex.DecodeString(recordedReservationHex)
			assert.NoError(t, err, "the recorded encoding must be hexadecimal")
			var got budget.Reservation
			assert.NoError(t, got.UnmarshalBinary(data), "UnmarshalBinary must decode the recorded encoding")
			assert.Equal(t, got, recordedReservation, "UnmarshalBinary must return the recorded Reservation")
		})

		t.Run("returns ErrUnknownImpact for an encoded Disruption whose Impact is not valid", func(t *testing.T) {
			t.Parallel()
			data, err := hex.DecodeString(unknownNestedImpactHex)
			assert.NoError(t, err, "the encoding must be hexadecimal")
			var got budget.Reservation
			assert.ErrorIs(t, got.UnmarshalBinary(data), budget.ErrUnknownImpact,
				"UnmarshalBinary must refuse a disruption with an impact outside the enumeration")
		})
	})
}
