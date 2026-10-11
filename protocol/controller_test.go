// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package protocol_test

import (
	"encoding/hex"
	"testing"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/protocol"
)

// recordedReceiptHex pins the encoding of recordedReceipt: field 1 is the incarnation,
// field 2 the applied flag, field 3 the generation 6, and field 4 the result {}.
const recordedReceiptHex = "0a10" + recordedIncarnationHex + "1001" + "1806" + "22027b7d"

// recordedDoneArgsHex pins the encoding of recordedDoneArgs: field 1 is the scope, field
// 2 the incarnation, and field 3 the disruption.
const recordedDoneArgsHex = "0a0c" + recordedScopeHex + "1210" + recordedIncarnationHex + "1a10" + recordedDisruptionHex

// recordedReceipt is the receipt whose encoding recordedReceiptHex pins.
var recordedReceipt = protocol.Receipt{
	Incarnation: recordedIncarnation,
	Applied:     true,
	Generation:  6,
	Result:      []byte(`{}`),
}

// recordedDoneArgs is the end of a reservation whose encoding recordedDoneArgsHex pins.
var recordedDoneArgs = protocol.DoneArgs{Scope: recordedScope, Incarnation: recordedIncarnation, ID: recordedDisruption}

// TestController checks the recorded encodings of a Receipt and a DoneArgs.
func TestController(t *testing.T) {
	t.Parallel()

	t.Run("Receipt", func(t *testing.T) {
		t.Parallel()

		t.Run("AppendBinary", func(t *testing.T) {
			t.Parallel()

			t.Run("appends the recorded encoding of a Receipt", func(t *testing.T) {
				t.Parallel()
				r := recordedReceipt
				got, err := r.AppendBinary(nil)
				assert.NoError(t, err, "AppendBinary must encode the Receipt")
				assert.Equal(
					t,
					hex.EncodeToString(got),
					recordedReceiptHex,
					"AppendBinary must write fields 1 to 4 as recorded",
				)
			})
		})

		t.Run("UnmarshalBinary", func(t *testing.T) {
			t.Parallel()

			t.Run("returns the Receipt of the recorded encoding", func(t *testing.T) {
				t.Parallel()
				data, err := hex.DecodeString(recordedReceiptHex)
				assert.NoError(t, err, "the recorded encoding must be hexadecimal")
				var got protocol.Receipt
				assert.NoError(t, got.UnmarshalBinary(data), "UnmarshalBinary must decode the recorded encoding")
				assert.Equal(t, got, recordedReceipt, "UnmarshalBinary must return the recorded Receipt")
			})
		})
	})

	t.Run("DoneArgs", func(t *testing.T) {
		t.Parallel()

		t.Run("AppendBinary", func(t *testing.T) {
			t.Parallel()

			t.Run("appends the recorded encoding of a DoneArgs", func(t *testing.T) {
				t.Parallel()
				a := recordedDoneArgs
				got, err := a.AppendBinary(nil)
				assert.NoError(t, err, "AppendBinary must encode the DoneArgs")
				assert.Equal(t, hex.EncodeToString(got), recordedDoneArgsHex,
					"AppendBinary must write fields 1 to 3 as recorded")
			})
		})
	})
}
