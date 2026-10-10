// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package proposal_test

import (
	"encoding/hex"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/kanon/kanontest"

	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/shard"
)

// The recorded encodings of proposals.
const (
	// recordedIncarnationHex is the encoding of the incarnation of recordedProposal:
	// the 16 bytes of its identifier.
	recordedIncarnationHex = "0102030405060708090a0b0c0d0e0f10"
	// recordedIDHex is the encoding of the ID of recordedProposal: the 16 bytes of its
	// identifier.
	recordedIDHex = "1112131415161718191a1b1c1d1e1f20"

	// recordedProposalHex pins the encoding of recordedProposal, which the scope of the
	// controller that applies it keeps. Field 1 is the scope, with the namespace orders
	// and the cell c1. Field 2 is the incarnation and field 3 the ID, each with the
	// length of its identifier. Field 4 is the kind move, 2, field 5 the base 7, and
	// field 6 the body {}. Field 7 is the expiry, one second after the Unix epoch in
	// UTC.
	recordedProposalHex = "0a0c" + "0a06" + "6f7264657273" + "1202" + "6331" +
		"1210" + recordedIncarnationHex +
		"1a10" + recordedIDHex +
		"2002" +
		"2807" +
		"3202" + "7b7d" +
		"3a02" + "0802"

	// unknownKindHex is field 4 with the kind 6, which no constant has.
	unknownKindHex = "2006"

	// recordedNamespace is the namespace of recordedProposal.
	recordedNamespace shard.Namespace = "orders"
	// recordedCell is the cell of recordedProposal.
	recordedCell shard.CellID = "c1"
	// recordedBody is the body of recordedProposal.
	recordedBody = "{}"
)

// recordedProposal is the proposal whose encoding recordedProposalHex pins.
var recordedProposal = proposal.Proposal{
	Scope: shard.Scope{Namespace: recordedNamespace, Cell: recordedCell},
	Incarnation: shard.NamespaceIncarnation{ID: id.New128([16]byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
	})},
	ID: proposal.ID{ID: id.New128([16]byte{
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
	})},
	Kind:    proposal.Move,
	Base:    7,
	Body:    []byte(recordedBody),
	Expires: time.Unix(1, 0).UTC(),
}

// TestProposal checks the recorded encoding of a Proposal, the refusal of a kind that
// is not Valid, and the guarantees of the exact type ID.
func TestProposal(t *testing.T) {
	t.Parallel()

	t.Run("AppendBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("appends the recorded encoding of a Proposal", func(t *testing.T) {
			t.Parallel()
			p := recordedProposal
			got, err := p.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode the Proposal")
			assert.Equal(t, hex.EncodeToString(got), recordedProposalHex,
				"AppendBinary must write fields 1 to 7 as recorded")
		})

		t.Run("returns ErrUnknownKind for a Kind that is not Valid", func(t *testing.T) {
			t.Parallel()
			p := proposal.Proposal{Kind: proposal.Explain + 1}
			_, err := p.AppendBinary(nil)
			assert.ErrorIs(t, err, proposal.ErrUnknownKind, "AppendBinary must refuse a kind outside the enumeration")
		})
	})

	t.Run("UnmarshalBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the Proposal of the recorded encoding", func(t *testing.T) {
			t.Parallel()
			data, err := hex.DecodeString(recordedProposalHex)
			assert.NoError(t, err, "the recorded encoding must be hexadecimal")
			var got proposal.Proposal
			assert.NoError(t, got.UnmarshalBinary(data), "UnmarshalBinary must decode the recorded encoding")
			assert.Equal(t, got, recordedProposal, "UnmarshalBinary must return the recorded Proposal")
		})

		t.Run("returns ErrUnknownKind for an encoded Kind that is not Valid", func(t *testing.T) {
			t.Parallel()
			data, err := hex.DecodeString(unknownKindHex)
			assert.NoError(t, err, "the encoding must be hexadecimal")
			var got proposal.Proposal
			assert.ErrorIs(t, got.UnmarshalBinary(data), proposal.ErrUnknownKind,
				"UnmarshalBinary must refuse a kind outside the enumeration")
		})
	})

	t.Run("ID", func(t *testing.T) {
		t.Parallel()

		t.Run("keeps the guarantees of an exact kanon type", func(t *testing.T) {
			t.Parallel()
			kanontest.RunExact[proposal.ID](t)
		})
	})
}
