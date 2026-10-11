// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package store_test

import (
	"encoding/hex"
	"testing"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/store"
)

// recordedHeadHex pins the encoding of recordedHead. Field 1 is the incarnation, with
// the length of its identifier. Field 2 is the generation 5, field 3 the watermark 3,
// and field 4 the schema 1.
const recordedHeadHex = "0a10" + recordedIncarnationHex + "1005" + "1803" + "2001"

// recordedHead is the head whose encoding recordedHeadHex pins.
var recordedHead = store.Head{
	Incarnation: recordedIncarnation,
	Generation:  5,
	Watermark:   3,
	Schema:      recordedSchema,
}

// TestHead checks the recorded encoding of a Head.
func TestHead(t *testing.T) {
	t.Parallel()

	t.Run("AppendBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("appends the recorded encoding of a Head", func(t *testing.T) {
			t.Parallel()
			h := recordedHead
			got, err := h.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode the Head")
			assert.Equal(
				t,
				hex.EncodeToString(got),
				recordedHeadHex,
				"AppendBinary must write fields 1 to 4 as recorded",
			)
		})
	})

	t.Run("UnmarshalBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the Head of the recorded encoding", func(t *testing.T) {
			t.Parallel()
			data, err := hex.DecodeString(recordedHeadHex)
			assert.NoError(t, err, "the recorded encoding must be hexadecimal")
			var got store.Head
			assert.NoError(t, got.UnmarshalBinary(data), "UnmarshalBinary must decode the recorded encoding")
			assert.Equal(t, got, recordedHead, "UnmarshalBinary must return the recorded Head")
		})
	})
}
