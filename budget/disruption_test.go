// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package budget_test

import (
	"encoding/hex"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/kanon/kanontest"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/shard"
)

// The recorded encodings of disruptions, and the values that they contain.
const (
	// recordedIDHex is the encoding of the ID of recordedDisruption: the 16 bytes of
	// its identifier.
	recordedIDHex = "2122232425262728292a2b2c2d2e2f30"
	// recordedIncarnationHex is the encoding of the namespace incarnation of
	// recordedDisruption: the 16 bytes of its identifier.
	recordedIncarnationHex = "0102030405060708090a0b0c0d0e0f10"

	// recordedDisruptionHex pins the encoding of recordedDisruption, which the scope of
	// its cell keeps in a reservation. Field 1 is the ID and field 3 the incarnation,
	// each with the length of its identifier. Field 2 is the scope, with the namespace
	// orders and the cell c1. Field 4 contains the node n1, field 5 is the node
	// incarnation 3, and field 6 is the impact process-restart, 2. Fields 7 and 8 are
	// the start and the end, one and two seconds after the Unix epoch in UTC. Field 9
	// is true for a negotiable disruption.
	recordedDisruptionHex = "0a10" + recordedIDHex +
		"120c" + "0a06" + "6f7264657273" + "1202" + "6331" +
		"1a10" + recordedIncarnationHex +
		"2203" + "02" + "6e31" +
		"2803" +
		"3002" +
		"3a02" + "0802" +
		"4202" + "0804" +
		"4801"

	// unknownImpactHex is field 6 with the impact 4, which no constant has.
	unknownImpactHex = "3004"

	// recordedNamespace is the namespace of recordedDisruption.
	recordedNamespace shard.Namespace = "orders"
	// recordedCell is the cell of recordedDisruption.
	recordedCell shard.CellID = "c1"
	// recordedNode is the node of recordedDisruption.
	recordedNode shard.NodeID = "n1"
	// recordedNodeIncarnation is the incarnation of recordedNode.
	recordedNodeIncarnation epoch.Epoch = 3
)

// recordedDisruption is the disruption whose encoding recordedDisruptionHex pins: the
// restart of the node n1, which the node requests for itself.
var recordedDisruption = budget.Disruption{
	ID: budget.ID{ID: id.New128([16]byte{
		0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
		0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30,
	})},
	Scope: shard.Scope{Namespace: recordedNamespace, Cell: recordedCell},
	Incarnation: shard.NamespaceIncarnation{ID: id.New128([16]byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
	})},
	Nodes:           []shard.NodeID{recordedNode},
	NodeIncarnation: recordedNodeIncarnation,
	Impact:          budget.ProcessRestart,
	Start:           time.Unix(1, 0).UTC(),
	End:             time.Unix(2, 0).UTC(),
	Negotiable:      true,
}

// TestDisruption checks the recorded encoding of a Disruption, the refusal of an
// impact that is not valid, and the guarantees of the exact type ID.
func TestDisruption(t *testing.T) {
	t.Parallel()

	t.Run("AppendBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("appends the recorded encoding of a Disruption", func(t *testing.T) {
			t.Parallel()
			d := recordedDisruption
			got, err := d.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode the Disruption")
			assert.Equal(t, hex.EncodeToString(got), recordedDisruptionHex,
				"AppendBinary must write fields 1 to 9 as recorded")
		})

		t.Run("returns ErrUnknownImpact for an Impact that is not valid", func(t *testing.T) {
			t.Parallel()
			d := budget.Disruption{Impact: budget.FullLoss + 1}
			_, err := d.AppendBinary(nil)
			assert.ErrorIs(t, err, budget.ErrUnknownImpact,
				"AppendBinary must refuse an impact outside the enumeration")
		})
	})

	t.Run("UnmarshalBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the Disruption of the recorded encoding", func(t *testing.T) {
			t.Parallel()
			data, err := hex.DecodeString(recordedDisruptionHex)
			assert.NoError(t, err, "the recorded encoding must be hexadecimal")
			var got budget.Disruption
			assert.NoError(t, got.UnmarshalBinary(data), "UnmarshalBinary must decode the recorded encoding")
			assert.Equal(t, got, recordedDisruption, "UnmarshalBinary must return the recorded Disruption")
		})

		t.Run("returns ErrUnknownImpact for an encoded Impact that is not valid", func(t *testing.T) {
			t.Parallel()
			data, err := hex.DecodeString(unknownImpactHex)
			assert.NoError(t, err, "the encoding must be hexadecimal")
			var got budget.Disruption
			assert.ErrorIs(t, got.UnmarshalBinary(data), budget.ErrUnknownImpact,
				"UnmarshalBinary must refuse an impact outside the enumeration")
		})
	})

	t.Run("ID", func(t *testing.T) {
		t.Parallel()

		t.Run("keeps the guarantees of an exact kanon type", func(t *testing.T) {
			t.Parallel()
			kanontest.RunExact[budget.ID](t)
		})
	})
}
