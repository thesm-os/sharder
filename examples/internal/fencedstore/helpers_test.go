// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package fencedstore_test

import (
	"context"

	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/examples/internal/fencedstore"
	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// The keys and the values of the tests.
const (
	// row is the key that the transactions of the tests write.
	row = "row"
	// value is the value that the transactions of the tests write.
	value = "value"
	// other is a second value of row.
	other = "other"
)

// The epochs of the tests.
const (
	// older is the epoch of a writer below the watermark.
	older epoch.Epoch = 2
	// newer is the epoch of the writer that sets the watermark.
	newer epoch.Epoch = 3
)

// The tokens and the callback key of the tests.
var (
	// tokenA is the token of the first shard of the tests at the epoch newer.
	tokenA = fence.Token{
		Namespace:   "orders",
		Cell:        "cell-1",
		Incarnation: shard.NamespaceIncarnation{ID: id.New128([id.Size128]byte{0: 0x1a, 15: 1})},
		Shard:       shard.ID{ID128: id.ID128{0: 0x02, 15: 1}},
		Epoch:       newer,
	}
	// keyA is the key of a callback of the first shard of the tests.
	keyA = protocol.Key{Node: "node-a", Step: 1, Epoch: newer}
)

// put returns a transaction that checks the fence of w and then puts v under row.
func put(ctx context.Context, w fence.Writer[*fencedstore.Tx], v string) func(tx *fencedstore.Tx) error {
	return func(tx *fencedstore.Tx) error {
		if err := w.Check(ctx, tx); err != nil {
			return err
		}
		tx.Put(row, v)
		return nil
	}
}
