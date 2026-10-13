// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package fencedstore_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/coretest/epochtest"
	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/examples/internal/fencedstore"
	"go.thesmos.sh/sharder/fence"
)

// handle is a writer of the fence laws with its store.
type handle struct {
	// s is the store of the writer.
	s *fencedstore.Store
	// w is the writer.
	w fence.Writer[*fencedstore.Tx]
}

// state is what the fence laws observe of a store: the number of its committed transactions
// and the value of row. Its fields are exported, because the suite of core compares states
// with go-cmp.
type state struct {
	// Value is the value of row, and empty without one.
	Value string
	// Position is the number of committed transactions.
	Position int
}

// TestWriter checks the fence laws against the writers of a store.
func TestWriter(t *testing.T) {
	t.Parallel()

	t.Run("Check", func(t *testing.T) {
		t.Parallel()

		t.Run("passes the fence laws", func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			epochtest.AssertFencedWriter(t, epochtest.FencedSystem[handle]{
				NewScope: func() epochtest.FencedScope[handle] {
					s := fencedstore.New()
					r := s.NewReplica()
					return epochtest.FencedScope[handle]{
						Open: func(e epoch.Epoch) (handle, error) {
							tok := tokenA
							tok.Epoch = e
							return handle{s: s, w: s.Writer(tok)}, nil
						},
						Supersede: func(e epoch.Epoch) {
							tok := tokenA
							tok.Epoch = e
							assert.NoError(t, s.Acquire(ctx, tok), "the acquisition of a newer tenure must succeed")
						},
						Snapshot: func() any {
							v, _ := s.Get(row)
							return state{Value: v, Position: r.Position()}
						},
					}
				},
				Writes: map[string]func(h handle) error{
					"acquire": func(h handle) error { return h.s.Acquire(ctx, h.w.Token()) },
					"put":     func(h handle) error { return h.s.Update(ctx, put(ctx, h.w, value)) },
				},
			})
		})

		t.Run("keeps the watermark of a transaction that does not commit", func(t *testing.T) {
			t.Parallel()
			s := fencedstore.New()
			err := s.Update(t.Context(), func(tx *fencedstore.Tx) error {
				assert.NoError(t, s.Writer(tokenA).Check(t.Context(), tx), "the newer writer must pass the fence")
				return errDecline
			})
			assert.ErrorIs(t, err, errDecline, "Update must return the error of the transaction")
			stale := tokenA
			stale.Epoch = older
			assert.NoError(t, s.Update(t.Context(), put(t.Context(), s.Writer(stale), value)),
				"the watermark of a transaction that did not commit must not fence an older writer")
		})
	})
}
