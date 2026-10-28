// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package fence

import (
	"context"
	"math"

	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/shard"
)

// Retired is the watermark at which fenced storage refuses every fenced write of a
// fence key, such as the watermark that an operator sets before a namespace is
// recreated over the data of the old incarnation. It is above every epoch that the
// core issues, so a fence refuses each writer of the key with epoch.ErrFenced. It fits
// a signed 64-bit integer, such as a BIGINT column of PostgreSQL.
const Retired = epoch.Epoch(math.MaxInt64)

// Token authorises writes to one shard at one epoch. Its fence key is the pair of
// Incarnation and Shard. Namespace and Cell identify the shard to an operator, and
// neither is part of the key, so a shard keeps its fence record when it changes
// cells.
//
// The zero Token is unfenced, because its Epoch is epoch.Zero.
type Token struct {
	// Namespace is the namespace of the shard.
	Namespace shard.Namespace
	// Cell is the cell of the shard when the core granted the token.
	Cell shard.CellID
	// Incarnation is the namespace incarnation of the shard, the first part of the
	// fence key.
	Incarnation shard.NamespaceIncarnation
	// Shard is the shard, the second part of the fence key.
	Shard shard.ID
	// Epoch is the epoch of the tenure that the token authorises.
	Epoch epoch.Epoch
}

// Acquirer starts the tenure of a writer.
//
//dokimi:stub
//dokimi:out path=fencetest/
type Acquirer interface {
	// Acquire advances the watermark of the fence key of t to t.Epoch in one
	// transaction. A writer calls it once per tenure, before it reads the data of its
	// shard. It returns epoch.ErrFenced when the watermark of the fence key is above
	// t.Epoch.
	Acquire(ctx context.Context, t Token) error
}

// Guard is the fence of one storage system. Tx is its transaction type, such as
// *sql.Tx.
type Guard[Tx any] interface {
	Acquirer
	// Writer returns a writer bound to t. It does not perform I/O.
	Writer(t Token) Writer[Tx]
}

// Writer checks the writes of one tenure against the fence.
type Writer[Tx any] interface {
	// Token returns the token that the writer is bound to.
	Token() Token
	// Check applies the fence laws to the epoch of the writer inside tx. It locks the
	// fence record against a new acquisition until tx ends, and advances the
	// watermark in tx when the epoch of the writer is above it. It returns
	// epoch.ErrFenced when the watermark is above the epoch of the writer. A writer
	// calls it in every transaction that writes the data of its shard.
	Check(ctx context.Context, tx Tx) error
}
