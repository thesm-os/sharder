// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package fencedstore_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/examples/internal/fencedstore"
)

// TestReplica checks the reads, the position and the pause of a replica.
func TestReplica(t *testing.T) {
	t.Parallel()

	t.Run("Get", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the values of the transactions that the store committed", func(t *testing.T) {
			t.Parallel()
			s := fencedstore.New()
			r := s.NewReplica()
			assert.NoError(t, s.Update(t.Context(), func(tx *fencedstore.Tx) error {
				tx.Put(row, value)
				return nil
			}), "the transaction must commit")
			got, ok := r.Get(row)
			assert.True(t, ok, "the replica must have the committed key")
			assert.Equal(t, got, value, "the replica must have the committed value")
		})
	})

	t.Run("Pause", func(t *testing.T) {
		t.Parallel()

		t.Run("keeps the replica at its position while the store commits", func(t *testing.T) {
			t.Parallel()
			s := fencedstore.New()
			r := s.NewReplica()
			r.Pause()
			assert.NoError(t, s.Update(t.Context(), func(tx *fencedstore.Tx) error {
				tx.Put(row, value)
				return nil
			}), "the transaction must commit")
			_, ok := r.Get(row)
			expect.False(t, ok, "a paused replica must not apply a later transaction")
			expect.Equal(t, r.Position(), 0, "a paused replica must keep its position")
		})
	})

	t.Run("Resume", func(t *testing.T) {
		t.Parallel()

		t.Run("applies the transactions that the store committed during the pause", func(t *testing.T) {
			t.Parallel()
			s := fencedstore.New()
			r := s.NewReplica()
			r.Pause()
			assert.NoError(t, s.Update(t.Context(), func(tx *fencedstore.Tx) error {
				tx.Put(row, value)
				return nil
			}), "the transaction must commit")
			r.Resume()
			expect.Equal(t, r.Position(), 1, "a resumed replica must apply the committed transaction")
			got, _ := r.Get(row)
			expect.Equal(t, got, value, "a resumed replica must have the committed value")
		})
	})

	t.Run("Reads", func(t *testing.T) {
		t.Parallel()

		t.Run("counts the reads of the replica", func(t *testing.T) {
			t.Parallel()
			r := fencedstore.New().NewReplica()
			r.Get(row)
			r.Get(row)
			assert.Equal(t, r.Reads(), 2, "the replica must count each read")
		})
	})
}
