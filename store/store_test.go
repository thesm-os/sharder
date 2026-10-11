// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package store_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// The identities that the recorded encodings of the tests contain.
const (
	// recordedIncarnationHex is the encoding of recordedIncarnation: the 16 bytes of
	// its identifier.
	recordedIncarnationHex = "0102030405060708090a0b0c0d0e0f10"
	// recordedShardHex is the encoding of recordedShard: the 16 bytes of its
	// identifier.
	recordedShardHex = "1112131415161718191a1b1c1d1e1f20"
	// recordedDisruptionHex is the encoding of recordedDisruption: the 16 bytes of its
	// identifier.
	recordedDisruptionHex = "2122232425262728292a2b2c2d2e2f30"
	// recordedProposalIDHex is the encoding of recordedProposalID: the 16 bytes of its
	// identifier.
	recordedProposalIDHex = "3132333435363738393a3b3c3d3e3f40"
	// recordedCallerHex is the encoding of recordedCaller: its UTF-8 bytes.
	recordedCallerHex = "7370696666653a2f2f6f72646572732f6f7073"

	// recordedSchema is the value of Schema in this release, which the heads of scopes
	// record.
	recordedSchema = 1

	// recordedNamespace is the namespace of the recorded scope.
	recordedNamespace shard.Namespace = "orders"
	// recordedCell is the cell of the recorded scope.
	recordedCell shard.CellID = "c1"
	// recordedFrom is the node that the recorded transition moves the replica from.
	recordedFrom shard.NodeID = "n1"
	// recordedTo is the node that the recorded transition moves the replica to.
	recordedTo shard.NodeID = "n2"
	// recordedCaller is the identity that requested the recorded drain and submitted
	// the recorded proposal.
	recordedCaller shard.Identity = "spiffe://orders/ops"
)

// The identities of the recorded values.
var (
	// recordedIncarnation is the namespace incarnation of the recorded scope.
	recordedIncarnation = shard.NamespaceIncarnation{ID: id.New128([16]byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
	})}
	// recordedShard is the shard of the recorded placement and transition.
	recordedShard = shard.ID{ID: id.New128([16]byte{
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
	})}
	// recordedDisruption is the disruption of the recorded drain and reservation.
	recordedDisruption = budget.ID{ID: id.New128([16]byte{
		0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
		0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30,
	})}
	// recordedProposalID is the ID of the recorded proposal.
	recordedProposalID = proposal.ID{ID: id.New128([16]byte{
		0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,
		0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0x40,
	})}
)

// TestStore checks the schema that this release records.
func TestStore(t *testing.T) {
	t.Parallel()

	t.Run("Schema", func(t *testing.T) {
		t.Parallel()

		t.Run("is the recorded schema of the release", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, store.Schema, recordedSchema, "Schema must be the schema that the heads of scopes record")
		})
	})
}
