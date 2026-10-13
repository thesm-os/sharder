// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package fencedstore

import (
	"context"
	"fmt"

	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/fence"
)

// writer checks the writes of one tenure against the fence of its store.
type writer struct {
	// token is the token of the tenure.
	token fence.Token
}

var _ fence.Writer[*Tx] = (*writer)(nil)

// Token returns the token of the tenure of w.
func (w *writer) Token() fence.Token {
	return w.token
}

// Check applies the fence laws to the epoch of w in tx. A writer at epoch.Zero is unfenced. A
// writer at or above the watermark passes, and tx advances the watermark to its epoch when tx
// commits. The store runs one transaction at a time, so no acquisition runs until tx ends.
//
// Error modes:
//   - an error that wraps epoch.ErrFenced when the watermark is above the epoch of w.
func (w *writer) Check(_ context.Context, tx *Tx) error {
	e := w.token.Epoch
	if e == epoch.Zero {
		return nil
	}
	k := fenceKey{incarnation: w.token.Incarnation, shard: w.token.Shard}
	watermark := max(tx.s.fences[k].watermark, tx.fences[k])
	if watermark > e {
		return fmt.Errorf("fencedstore: a write of shard %v at epoch %d below %d: %w", w.token.Shard, e, watermark,
			epoch.ErrFenced)
	}
	tx.fences[k] = e
	return nil
}
