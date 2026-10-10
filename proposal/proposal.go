// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package proposal

//go:generate go tool kanon -type=Proposal

import (
	"time"

	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/shard"
)

// ID is the identity of a proposal in a namespace incarnation. The operator's tool
// assigns it with core's id/ulid when it builds the proposal.
//
// The zero ID is no proposal.
//
// # Encoding
//
// An ID encodes through the binary methods of the [id.ID] that it embeds. The
// encoding is the 16 bytes of the ULID, and the zero ID encodes as no bytes.
//
// # Concurrency
//
// An ID is a comparable value. A copy is independent of the original.
type ID struct{ id.ID }

// Proposal is one change that an operator requests. A controller applies it at most
// once, and only while the state that it changes is at Base.
//
// The zero Proposal is no proposal.
//
// # Encoding
//
// kanon generates the codec of Proposal. Scope is field 1, Incarnation is field 2, ID
// is field 3, Kind is field 4, Base is field 5, Body is field 6, and Expires is field
// 7. The codec returns an error that wraps [ErrUnknownKind] for a Kind other than zero
// that is not Valid.
//
// # Concurrency
//
// A copy of a Proposal shares the bytes of Body with the original.
type Proposal struct {
	// Scope is the scope whose controller applies the proposal: the root scope for a
	// spec, and the scope of a cell for an operation on the shards or the nodes of
	// the cell.
	Scope shard.Scope
	// Expires is the last instant at which a controller may apply the proposal.
	Expires time.Time
	// Body is the new spec, or the arguments of the operation, as JSON.
	Body []byte
	// Base is the version of the state that the proposal changes, such as the epoch
	// of a shard for a move.
	Base epoch.Epoch
	// Incarnation is the namespace incarnation that the proposal changes. It is zero
	// only in the spec proposal that creates the namespace, and a controller refuses
	// a proposal for another incarnation.
	Incarnation shard.NamespaceIncarnation
	// ID is unique within the namespace incarnation. A controller refuses an ID that
	// its scope has applied.
	ID ID
	// Kind is the kind of the change.
	Kind OperationKind
}
