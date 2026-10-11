// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package store_test

import (
	"encoding/hex"
	"testing"
	"time"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/store"
)

// The recorded encodings of drains.
const (
	// recordedDrainKeyHex pins the encoding of recordedDrainKey. Field 1 is the node
	// n1, and field 2 the disruption, with the length of its identifier.
	recordedDrainKeyHex = "0a02" + "6e31" + "1210" + recordedDisruptionHex

	// recordedDrainHex pins the encoding of recordedDrain. Fields 1 and 2 are those of
	// its key. Field 3 is the caller, and field 4 the start, two seconds after the Unix
	// epoch in UTC.
	recordedDrainHex = recordedDrainKeyHex + "1a13" + recordedCallerHex + "2202" + "0804"
)

// The recorded drain and its key.
var (
	// recordedDrainKey is the key whose encoding recordedDrainKeyHex pins.
	recordedDrainKey = store.DrainKey{Node: recordedFrom, Disruption: recordedDisruption}
	// recordedDrain is the drain whose encoding recordedDrainHex pins: the drain of n1
	// for the recorded disruption.
	recordedDrain = store.Drain{
		Node:       recordedFrom,
		Disruption: recordedDisruption,
		Caller:     recordedCaller,
		Since:      time.Unix(2, 0).UTC(),
	}
)

// TestDrain checks the recorded encodings of a Drain and a DrainKey.
func TestDrain(t *testing.T) {
	t.Parallel()

	t.Run("AppendBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("appends the recorded encoding of a Drain", func(t *testing.T) {
			t.Parallel()
			d := recordedDrain
			got, err := d.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode the Drain")
			assert.Equal(
				t,
				hex.EncodeToString(got),
				recordedDrainHex,
				"AppendBinary must write fields 1 to 4 as recorded",
			)
		})
	})

	t.Run("UnmarshalBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the Drain of the recorded encoding", func(t *testing.T) {
			t.Parallel()
			data, err := hex.DecodeString(recordedDrainHex)
			assert.NoError(t, err, "the recorded encoding must be hexadecimal")
			var got store.Drain
			assert.NoError(t, got.UnmarshalBinary(data), "UnmarshalBinary must decode the recorded encoding")
			assert.Equal(t, got, recordedDrain, "UnmarshalBinary must return the recorded Drain")
		})
	})

	t.Run("DrainKey", func(t *testing.T) {
		t.Parallel()

		t.Run("AppendBinary", func(t *testing.T) {
			t.Parallel()

			t.Run("appends the recorded encoding of a DrainKey", func(t *testing.T) {
				t.Parallel()
				k := recordedDrainKey
				got, err := k.AppendBinary(nil)
				assert.NoError(t, err, "AppendBinary must encode the DrainKey")
				assert.Equal(t, hex.EncodeToString(got), recordedDrainKeyHex,
					"AppendBinary must write fields 1 and 2 as recorded")
			})
		})

		t.Run("UnmarshalBinary", func(t *testing.T) {
			t.Parallel()

			t.Run("returns the DrainKey of the recorded encoding", func(t *testing.T) {
				t.Parallel()
				data, err := hex.DecodeString(recordedDrainKeyHex)
				assert.NoError(t, err, "the recorded encoding must be hexadecimal")
				var got store.DrainKey
				assert.NoError(t, got.UnmarshalBinary(data), "UnmarshalBinary must decode the recorded encoding")
				assert.Equal(t, got, recordedDrainKey, "UnmarshalBinary must return the recorded DrainKey")
			})
		})
	})
}
