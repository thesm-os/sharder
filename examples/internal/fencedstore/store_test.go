// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package fencedstore_test

import (
	"context"
	"errors"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/examples/internal/fencedstore"
)

// errDecline is the error of a transaction of the tests that declines to commit.
var errDecline = errors.New("test: the transaction declines")

// TestStore checks the acquisitions, the transactions, the reads and the readers of a store.
func TestStore(t *testing.T) {
	t.Parallel()

	t.Run("Acquire", func(t *testing.T) {
		t.Parallel()

		t.Run("returns nil for a token at the watermark of its shard", func(t *testing.T) {
			t.Parallel()
			s := fencedstore.New()
			assert.NoError(t, s.Acquire(t.Context(), tokenA), "the first acquisition must succeed")
			assert.NoError(t, s.Acquire(t.Context(), tokenA), "an acquisition at the watermark must succeed")
		})

		t.Run("returns an error that wraps epoch.ErrFenced for a token below the watermark of its shard",
			func(t *testing.T) {
				t.Parallel()
				s := fencedstore.New()
				assert.NoError(t, s.Acquire(t.Context(), tokenA), "the acquisition of the newer tenure must succeed")
				stale := tokenA
				stale.Epoch = older
				assert.ErrorIs(t, s.Acquire(t.Context(), stale), epoch.ErrFenced,
					"an acquisition below the watermark must be fenced")
			})

		t.Run("returns nil for a token at epoch.Zero", func(t *testing.T) {
			t.Parallel()
			s := fencedstore.New()
			assert.NoError(t, s.Acquire(t.Context(), tokenA), "the acquisition of the newer tenure must succeed")
			unfenced := tokenA
			unfenced.Epoch = epoch.Zero
			assert.NoError(t, s.Acquire(t.Context(), unfenced), "an unfenced acquisition must succeed")
		})

		t.Run("keeps the fence of each shard apart", func(t *testing.T) {
			t.Parallel()
			s := fencedstore.New()
			assert.NoError(t, s.Acquire(t.Context(), tokenA), "the acquisition of the first shard must succeed")
			second := tokenA
			second.Shard.ID128[15] = 2
			second.Epoch = older
			assert.NoError(t, s.Acquire(t.Context(), second), "the fence of the first shard must not fence the second")
		})

		t.Run("returns the cause of its context when the context has ended", func(t *testing.T) {
			t.Parallel()
			s := fencedstore.New()
			assert.HonoursCancellation(t, func(ctx context.Context) error { return s.Acquire(ctx, tokenA) },
				"Acquire must return the error of an ended context")
		})
	})

	t.Run("Writer", func(t *testing.T) {
		t.Parallel()

		t.Run("returns a writer bound to the token", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, fencedstore.New().Writer(tokenA).Token(), tokenA, "the writer must have its token")
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Parallel()

		t.Run("commits the writes of a transaction that returns nil", func(t *testing.T) {
			t.Parallel()
			s := fencedstore.New()
			assert.NoError(t, s.Update(t.Context(), func(tx *fencedstore.Tx) error {
				tx.Put(row, value)
				return nil
			}), "the transaction must commit")
			got, ok := s.Get(row)
			assert.True(t, ok, "the store must have the committed key")
			assert.Equal(t, got, value, "the store must have the committed value")
		})

		t.Run("discards the writes of a transaction that returns an error", func(t *testing.T) {
			t.Parallel()
			s := fencedstore.New()
			err := s.Update(t.Context(), func(tx *fencedstore.Tx) error {
				tx.Put(row, value)
				tx.Record(&keyA)
				return errDecline
			})
			assert.ErrorIs(t, err, errDecline, "Update must return the error of the transaction")
			_, ok := s.Get(row)
			expect.False(t, ok, "the store must not have the value of a declined transaction")
			expect.NoError(t, s.Update(t.Context(), func(tx *fencedstore.Tx) error {
				expect.False(t, tx.Recorded(&keyA), "the store must not have the key of a declined transaction")
				return nil
			}), "the transaction that reads the keys must commit")
		})

		t.Run("returns the cause of its context when the context has ended", func(t *testing.T) {
			t.Parallel()
			s := fencedstore.New()
			assert.HonoursCancellation(t, func(ctx context.Context) error {
				return s.Update(ctx, func(*fencedstore.Tx) error { return nil })
			}, "Update must return the error of an ended context")
		})
	})

	t.Run("Get", func(t *testing.T) {
		t.Parallel()

		t.Run("reports false for a key without a value", func(t *testing.T) {
			t.Parallel()
			_, ok := fencedstore.New().Get(row)
			assert.False(t, ok, "a new store must not have a value")
		})
	})

	t.Run("Reader", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the replica when the replica has applied the acquisition of the token", func(t *testing.T) {
			t.Parallel()
			s := fencedstore.New()
			r := s.NewReplica()
			assert.NoError(t, s.Acquire(t.Context(), tokenA), "the acquisition must succeed")
			assert.Equal(t, s.Reader(&tokenA, r), fencedstore.Reader(r),
				"the store must return a replica that has applied the acquisition", assert.ByIdentity())
		})

		t.Run("returns the store when the replica has not applied the acquisition of the token", func(t *testing.T) {
			t.Parallel()
			s := fencedstore.New()
			r := s.NewReplica()
			r.Pause()
			assert.NoError(t, s.Acquire(t.Context(), tokenA), "the acquisition must succeed")
			assert.Equal(t, s.Reader(&tokenA, r), fencedstore.Reader(s),
				"the store must return itself for a replica that has not applied the acquisition", assert.ByIdentity())
		})
	})
}
