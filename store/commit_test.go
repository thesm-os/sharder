// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package store_test

import (
	"encoding/hex"
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/crypto"

	"go.thesmos.sh/sharder/store"
)

// The recorded encodings of commit IDs and receipts.
const (
	// recordedCommitIDHex pins the encoding of recordedCommitID. Field 1 is the
	// incarnation, with the length of its identifier. Field 2 is the term 3, and field
	// 3 the sequence number 2.
	recordedCommitIDHex = "0a10" + recordedIncarnationHex + "1003" + "1802"

	// recordedReceiptHex pins the encoding of recordedReceipt. Field 1 is the commit
	// ID, 22 bytes long. Field 2 is the digest of recordedMutation, and field 3 the
	// generation 5.
	recordedReceiptHex = "0a16" + recordedCommitIDHex + "1220" + recordedDigestHex + "1805"
)

// The recorded commit ID and receipt.
var (
	// recordedCommitID is the commit ID whose encoding recordedCommitIDHex pins: the
	// second commit of the term 3.
	recordedCommitID = store.CommitID{Incarnation: recordedIncarnation, Term: 3, Seq: 2}
	// recordedReceipt is the receipt whose encoding recordedReceiptHex pins: the
	// receipt of recordedMutation at the generation 5.
	recordedReceipt = store.Receipt{ID: recordedCommitID, Digest: recordedDigest, Generation: 5}
	// recordedDigest is the SHA-256 digest of the bytes of recordedMutationHex.
	recordedDigest = crypto.NewDigest256([crypto.DigestSize256]byte{
		0x80, 0x13, 0xc6, 0xa3, 0x67, 0x6f, 0x27, 0x3f,
		0x00, 0x8d, 0x2c, 0x1f, 0x71, 0x09, 0x94, 0x50,
		0x93, 0x3f, 0x39, 0x3f, 0x11, 0x6b, 0x79, 0x92,
		0xd7, 0x1b, 0xc4, 0x84, 0xa5, 0x87, 0x81, 0x7c,
	})
)

// TestCommit checks the recorded encodings of a CommitID and a Receipt.
func TestCommit(t *testing.T) {
	t.Parallel()

	t.Run("CommitID", func(t *testing.T) {
		t.Parallel()

		t.Run("AppendBinary", func(t *testing.T) {
			t.Parallel()

			t.Run("appends the recorded encoding of a CommitID", func(t *testing.T) {
				t.Parallel()
				c := recordedCommitID
				got, err := c.AppendBinary(nil)
				assert.NoError(t, err, "AppendBinary must encode the CommitID")
				assert.Equal(t, hex.EncodeToString(got), recordedCommitIDHex,
					"AppendBinary must write fields 1 to 3 as recorded")
			})
		})

		t.Run("UnmarshalBinary", func(t *testing.T) {
			t.Parallel()

			t.Run("returns the CommitID of the recorded encoding", func(t *testing.T) {
				t.Parallel()
				data, err := hex.DecodeString(recordedCommitIDHex)
				assert.NoError(t, err, "the recorded encoding must be hexadecimal")
				var got store.CommitID
				assert.NoError(t, got.UnmarshalBinary(data), "UnmarshalBinary must decode the recorded encoding")
				assert.Equal(t, got, recordedCommitID, "UnmarshalBinary must return the recorded CommitID")
			})
		})
	})

	t.Run("Receipt", func(t *testing.T) {
		t.Parallel()

		t.Run("AppendBinary", func(t *testing.T) {
			t.Parallel()

			t.Run("appends the recorded encoding of a Receipt", func(t *testing.T) {
				t.Parallel()
				r := recordedReceipt
				got, err := r.AppendBinary(nil)
				assert.NoError(t, err, "AppendBinary must encode the Receipt")
				assert.Equal(t, hex.EncodeToString(got), recordedReceiptHex,
					"AppendBinary must write fields 1 to 3 as recorded")
			})
		})

		t.Run("UnmarshalBinary", func(t *testing.T) {
			t.Parallel()

			t.Run("returns the Receipt of the recorded encoding", func(t *testing.T) {
				t.Parallel()
				data, err := hex.DecodeString(recordedReceiptHex)
				assert.NoError(t, err, "the recorded encoding must be hexadecimal")
				var got store.Receipt
				assert.NoError(t, got.UnmarshalBinary(data), "UnmarshalBinary must decode the recorded encoding")
				assert.Equal(t, got, recordedReceipt, "UnmarshalBinary must return the recorded Receipt")
			})
		})
	})
}
