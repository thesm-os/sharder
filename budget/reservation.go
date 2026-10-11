// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package budget

//go:generate go tool kanon -type=Reservation

import (
	"time"

	"go.thesmos.sh/core/crypto"
	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/shard"
)

// Reservation is the record of a disruption that a Budget deferred or admitted, in the
// scope of the cell of the disruption. A reservation is pending while Admitted is zero.
// Only an admitted reservation counts against the budget of its nodes.
//
// The zero Reservation is no reservation.
//
// # Encoding
//
// kanon generates the codec of Reservation. Disruption is field 1, Digest is field 2,
// Owner is field 3, Incarnations is field 4, and Admitted is field 5.
//
// # Concurrency
//
// A copy of a Reservation shares the map of Incarnations and the elements of the Nodes
// of its Disruption with the original.
type Reservation struct {
	// Admitted is the instant of the admission, or zero while the reservation is
	// pending.
	Admitted time.Time
	// Incarnations is the incarnation of each node of the disruption when the
	// controller committed the reservation.
	Incarnations map[shard.NodeID]epoch.Epoch
	// Owner is the identity of the caller that requested the disruption.
	Owner shard.Identity
	// Disruption is the reserved disruption.
	Disruption Disruption
	// Digest is the SHA-256 digest of the encoding of the disruption. Admit returns an
	// error that wraps ErrReusedID for a disruption with the same ID and another
	// digest.
	Digest crypto.Digest
}
