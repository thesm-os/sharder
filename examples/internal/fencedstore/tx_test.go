// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package fencedstore_test

import (
	"testing"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/examples/internal/fencedstore"
)

// TestTx checks the reads, the writes and the callback keys of a transaction.
func TestTx(t *testing.T) {
	t.Parallel()

	t.Run("Get", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the value that the transaction put", func(t *testing.T) {
			t.Parallel()
			s := fencedstore.New()
			assert.NoError(t, s.Update(t.Context(), func(tx *fencedstore.Tx) error {
				tx.Put(row, value)
				tx.Put(row, other)
				got, ok := tx.Get(row)
				assert.True(t, ok, "the transaction must have the key that it put")
				assert.Equal(t, got, other, "the transaction must have the last value that it put")
				return nil
			}), "the transaction must commit")
		})

		t.Run("returns the committed value of a key that the transaction did not put", func(t *testing.T) {
			t.Parallel()
			s := fencedstore.New()
			assert.NoError(t, s.Update(t.Context(), func(tx *fencedstore.Tx) error {
				tx.Put(row, value)
				return nil
			}), "the first transaction must commit")
			assert.NoError(t, s.Update(t.Context(), func(tx *fencedstore.Tx) error {
				got, ok := tx.Get(row)
				assert.True(t, ok, "the transaction must have the committed key")
				assert.Equal(t, got, value, "the transaction must have the committed value")
				return nil
			}), "the second transaction must commit")
		})
	})

	t.Run("Recorded", func(t *testing.T) {
		t.Parallel()

		t.Run("reports true for a key that the transaction recorded", func(t *testing.T) {
			t.Parallel()
			s := fencedstore.New()
			assert.NoError(t, s.Update(t.Context(), func(tx *fencedstore.Tx) error {
				tx.Record(&keyA)
				assert.True(t, tx.Recorded(&keyA), "the transaction must have the key that it recorded")
				return nil
			}), "the transaction must commit")
		})

		t.Run("reports true for a key that a committed transaction recorded", func(t *testing.T) {
			t.Parallel()
			s := fencedstore.New()
			assert.NoError(t, s.Update(t.Context(), func(tx *fencedstore.Tx) error {
				tx.Record(&keyA)
				return nil
			}), "the first transaction must commit")
			assert.NoError(t, s.Update(t.Context(), func(tx *fencedstore.Tx) error {
				assert.True(t, tx.Recorded(&keyA), "a later transaction must have the committed key")
				return nil
			}), "the second transaction must commit")
		})

		t.Run("reports false for a key that no transaction recorded", func(t *testing.T) {
			t.Parallel()
			s := fencedstore.New()
			assert.NoError(t, s.Update(t.Context(), func(tx *fencedstore.Tx) error {
				assert.False(t, tx.Recorded(&keyA), "a new store must not have a callback key")
				return nil
			}), "the transaction must commit")
		})
	})
}
