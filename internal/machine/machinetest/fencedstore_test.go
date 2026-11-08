// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machinetest_test

import (
	"fmt"
	"strconv"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/coretest/epochtest"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/internal/machine/machinetest"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// The key and the value of the writes of the tests.
const (
	keyA   = "a"
	value1 = "1"
)

// The shard, the namespace incarnation and the callbacks of the tests.
var (
	// shardA is the shard of the fence key of the tests.
	shardA = shard.ID{ID128: id.ID128{0: 0x5d, 15: 0x01}}
	// incarnation is the namespace incarnation of the fence key of the tests.
	incarnation = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: 0x1a, 15: 0x0a})}
	// addRequest is a callback that adds the shard to a node.
	addRequest = protocol.Request{
		Scope:     shard.Scope{Namespace: "orders", Cell: "cell-1"},
		Operation: shard.OperationID{Incarnation: incarnation, Shard: shardA, Sequence: 1},
		Call:      protocol.Add,
		Node:      "node-a",
	}
	// dropRequest is a callback that drops the shard from the node.
	dropRequest = protocol.Request{
		Scope:     shard.Scope{Namespace: "orders", Cell: "cell-1"},
		Operation: shard.OperationID{Incarnation: incarnation, Shard: shardA, Sequence: 1},
		Call:      protocol.Drop,
		Node:      "node-a",
		Step:      1,
	}
)

// handle is a writer of core's fence laws: a store and the token of the tenure that writes.
type handle struct {
	// s is the store of the writer.
	s *machinetest.FencedStore
	// t is the token of the tenure of the writer.
	t *fence.Token
}

// TestFencedStore checks the acquisitions, the write transactions, the tenures, the callback
// keys and the records of a FencedStore, and the fence laws of its writes.
func TestFencedStore(t *testing.T) {
	t.Parallel()

	t.Run("Acquire", func(t *testing.T) {
		t.Parallel()

		t.Run("returns an acquisition that ends at once without an open transaction", func(t *testing.T) {
			t.Parallel()
			done, err := machinetest.NewFencedStore().Acquire(token(2)).Result()
			expect.True(t, done, "the acquisition ends at once")
			expect.NoError(t, err, "the acquisition raises the watermark")
		})

		t.Run("returns ErrFenced for an epoch below the watermark", func(t *testing.T) {
			t.Parallel()
			s := machinetest.NewFencedStore()
			acquire(t, s, 3)
			done, err := s.Acquire(token(2)).Result()
			expect.True(t, done, "the acquisition ends at once")
			expect.ErrorIs(t, err, epoch.ErrFenced, "the acquisition of an epoch below the watermark fails")
		})

		t.Run("waits for each write transaction that checked the fence before it", func(t *testing.T) {
			t.Parallel()
			s := machinetest.NewFencedStore()
			tx := begin(t, s, 1)
			a := s.Acquire(token(2))
			done, _ := a.Result()
			assert.False(t, done, "the acquisition waits while a transaction that checked the fence is open")
			assert.NoError(t, tx.Commit(), "the transaction commits before the acquisition")
			done, err := a.Result()
			expect.True(t, done, "the acquisition ends once the transaction has ended")
			expect.NoError(t, err, "the acquisition raises the watermark once the transaction has ended")
		})

		t.Run("raises the watermark to the epoch of the token", func(t *testing.T) {
			t.Parallel()
			s := machinetest.NewFencedStore()
			acquire(t, s, 2)
			_, err := s.Begin(token(1))
			assert.ErrorIs(t, err, epoch.ErrFenced, "a transaction of a tenure below the acquisition fails")
		})

		t.Run("records the acquisition", func(t *testing.T) {
			t.Parallel()
			s := machinetest.NewFencedStore()
			acquire(t, s, 2)
			assert.Equal(t, s.Records(), []machinetest.Record{{Kind: machinetest.Acquired, Token: *token(2)}},
				"the store records the acquisition with its token")
		})

		t.Run("leaves the watermark for a token at epoch.Zero", func(t *testing.T) {
			t.Parallel()
			s := machinetest.NewFencedStore()
			acquire(t, s, epoch.Zero)
			begin(t, s, 1)
		})
	})

	t.Run("Begin", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrFenced for a tenure below an acquisition that waits", func(t *testing.T) {
			t.Parallel()
			s := machinetest.NewFencedStore()
			begin(t, s, 1)
			s.Acquire(token(2))
			_, err := s.Begin(token(1))
			assert.ErrorIs(t, err, epoch.ErrFenced,
				"a transaction of a tenure below an acquisition that waits fails")
		})

		t.Run("returns ErrEnded for a tenure that RevokeShard ended", func(t *testing.T) {
			t.Parallel()
			s := machinetest.NewFencedStore()
			s.RevokeShard(token(1))
			_, err := s.Begin(token(1))
			assert.ErrorIs(t, err, machinetest.ErrEnded, "a transaction of an ended tenure fails")
		})

		t.Run("admits a tenure at epoch.Zero below the watermark", func(t *testing.T) {
			t.Parallel()
			s := machinetest.NewFencedStore()
			acquire(t, s, 2)
			tx := begin(t, s, epoch.Zero)
			assert.NoError(t, tx.Commit(), "an unfenced transaction commits below the watermark")
		})
	})

	t.Run("AddShard", func(t *testing.T) {
		t.Parallel()

		t.Run("records the start of the tenure", func(t *testing.T) {
			t.Parallel()
			s := machinetest.NewFencedStore()
			assert.NoError(t, s.AddShard(&addRequest, token(2)), "AddShard starts the tenure")
			assert.Equal(t, s.Records(), []machinetest.Record{{Kind: machinetest.Started, Token: *token(2)}},
				"the store records the start of the tenure")
		})

		t.Run("applies the effect of a callback key once", func(t *testing.T) {
			t.Parallel()
			s := machinetest.NewFencedStore()
			assert.NoError(t, s.AddShard(&addRequest, token(2)), "AddShard starts the tenure")
			assert.NoError(t, s.AddShard(&addRequest, token(2)), "AddShard returns nil for a recorded key")
			k := addRequest.Key()
			assert.Equal(t, s.Effects(&k), 1, "the store applies the effect of a key once")
		})

		t.Run("returns ErrFenced for a token below the watermark", func(t *testing.T) {
			t.Parallel()
			s := machinetest.NewFencedStore()
			acquire(t, s, 3)
			assert.ErrorIs(t, s.AddShard(&addRequest, token(2)), epoch.ErrFenced,
				"AddShard of a tenure below the watermark fails")
		})

		t.Run("returns ErrEnded for a tenure that RevokeShard ended", func(t *testing.T) {
			t.Parallel()
			s := machinetest.NewFencedStore()
			s.RevokeShard(token(2))
			assert.ErrorIs(t, s.AddShard(&addRequest, token(2)), machinetest.ErrEnded,
				"AddShard of an ended tenure fails")
		})
	})

	t.Run("DropShard", func(t *testing.T) {
		t.Parallel()

		t.Run("records the end of the tenure", func(t *testing.T) {
			t.Parallel()
			s := machinetest.NewFencedStore()
			assert.NoError(t, s.AddShard(&addRequest, token(2)), "AddShard starts the tenure")
			s.DropShard(&dropRequest, token(2))
			assert.Equal(t, s.Records(), []machinetest.Record{
				{Kind: machinetest.Started, Token: *token(2)},
				{Kind: machinetest.Ended, Token: *token(2)},
			}, "the store records the start and the end of the tenure in order")
		})

		t.Run("ends the open transactions of the tenure", func(t *testing.T) {
			t.Parallel()
			s := machinetest.NewFencedStore()
			tx := begin(t, s, 2)
			s.DropShard(&dropRequest, token(2))
			assert.ErrorIs(t, tx.Commit(), machinetest.ErrEnded,
				"a transaction of the tenure that DropShard ended does not commit")
		})

		t.Run("applies the effect of a callback key once", func(t *testing.T) {
			t.Parallel()
			s := machinetest.NewFencedStore()
			s.DropShard(&dropRequest, token(2))
			s.DropShard(&dropRequest, token(2))
			k := dropRequest.Key()
			assert.Equal(t, s.Effects(&k), 1, "the store applies the effect of a key once")
		})
	})

	t.Run("RevokeShard", func(t *testing.T) {
		t.Parallel()

		t.Run("ends the open transactions of the tenure", func(t *testing.T) {
			t.Parallel()
			s := machinetest.NewFencedStore()
			tx := begin(t, s, 2)
			s.RevokeShard(token(2))
			assert.ErrorIs(t, tx.Commit(), machinetest.ErrEnded,
				"a transaction of the tenure that RevokeShard ended does not commit")
		})

		t.Run("keeps the open transactions of another tenure", func(t *testing.T) {
			t.Parallel()
			s := machinetest.NewFencedStore()
			tx := begin(t, s, 2)
			s.RevokeShard(token(1))
			assert.NoError(t, tx.Commit(), "a transaction of another tenure commits")
		})

		t.Run("records the end of a tenure that runs", func(t *testing.T) {
			t.Parallel()
			s := machinetest.NewFencedStore()
			assert.NoError(t, s.AddShard(&addRequest, token(2)), "AddShard starts the tenure")
			s.RevokeShard(token(2))
			assert.Equal(t, s.Records(), []machinetest.Record{
				{Kind: machinetest.Started, Token: *token(2)},
				{Kind: machinetest.Ended, Token: *token(2)},
			}, "the store records the end of the tenure that RevokeShard ended")
		})

		t.Run("records nothing for a tenure that does not run", func(t *testing.T) {
			t.Parallel()
			s := machinetest.NewFencedStore()
			s.RevokeShard(token(2))
			assert.Empty(t, s.Records(), "the store records no end of a tenure that never started")
		})
	})

	t.Run("Calls", func(t *testing.T) {
		t.Parallel()

		t.Run("counts each call of a callback key", func(t *testing.T) {
			t.Parallel()
			s := machinetest.NewFencedStore()
			assert.NoError(t, s.AddShard(&addRequest, token(2)), "AddShard starts the tenure")
			assert.NoError(t, s.AddShard(&addRequest, token(2)), "AddShard returns nil for a recorded key")
			k := addRequest.Key()
			assert.Equal(t, s.Calls(&k), 2, "the store counts each call of the key")
		})
	})

	t.Run("Acquisition", func(t *testing.T) {
		t.Parallel()

		t.Run("Result", func(t *testing.T) {
			t.Parallel()

			t.Run("returns ErrFenced for an acquisition that a newer tenure passed while it waited",
				func(t *testing.T) {
					t.Parallel()
					s := machinetest.NewFencedStore()
					older := begin(t, s, 1)
					a := s.Acquire(token(2))
					assert.NoError(t, begin(t, s, 3).Commit(), "the transaction of the newer tenure commits")
					assert.ErrorIs(t, older.Commit(), epoch.ErrFenced,
						"the transaction of the older tenure fails after the newer tenure committed")
					done, err := a.Result()
					expect.True(t, done, "the acquisition ends once the transaction that it waits for has ended")
					expect.ErrorIs(t, err, epoch.ErrFenced, "the acquisition fails below the newer tenure")
				})
		})
	})

	t.Run("Txn", func(t *testing.T) {
		t.Parallel()

		t.Run("Commit", func(t *testing.T) {
			t.Parallel()

			fences(t)

			t.Run("records the writes of the transaction with its tenure", func(t *testing.T) {
				t.Parallel()
				s := machinetest.NewFencedStore()
				tx := begin(t, s, 1)
				tx.Put(keyA, value1)
				assert.NoError(t, tx.Commit(), "the transaction commits")
				assert.Equal(t, s.Records(), []machinetest.Record{
					{Kind: machinetest.Committed, Token: *token(1), Key: keyA, Value: value1},
				}, "the store records the write with the token of its tenure")
			})

			t.Run("returns ErrFenced once a newer tenure has committed", func(t *testing.T) {
				t.Parallel()
				s := machinetest.NewFencedStore()
				older, newer := begin(t, s, 1), begin(t, s, 2)
				assert.NoError(t, newer.Commit(), "the transaction of the newer tenure commits")
				assert.ErrorIs(t, older.Commit(), epoch.ErrFenced,
					"the transaction of the older tenure fails after the newer tenure committed")
			})

			t.Run("returns ErrEnded for a transaction that has ended", func(t *testing.T) {
				t.Parallel()
				tx := begin(t, machinetest.NewFencedStore(), 1)
				assert.NoError(t, tx.Commit(), "the transaction commits")
				assert.ErrorIs(t, tx.Commit(), machinetest.ErrEnded, "a second commit fails")
			})
		})

		t.Run("Abort", func(t *testing.T) {
			t.Parallel()

			t.Run("records no write", func(t *testing.T) {
				t.Parallel()
				s := machinetest.NewFencedStore()
				tx := begin(t, s, 1)
				tx.Put(keyA, value1)
				tx.Abort()
				assert.Empty(t, s.Records(), "the store records no write of an aborted transaction")
			})

			t.Run("ends an acquisition that waits for the transaction", func(t *testing.T) {
				t.Parallel()
				s := machinetest.NewFencedStore()
				tx := begin(t, s, 1)
				a := s.Acquire(token(2))
				tx.Abort()
				done, err := a.Result()
				expect.True(t, done, "the acquisition ends once the transaction has ended")
				expect.NoError(t, err, "the acquisition raises the watermark once the transaction has ended")
			})
		})
	})
}

// token returns the fencing token of the shard of the tests at the epoch e.
func token(e epoch.Epoch) *fence.Token {
	return &fence.Token{Namespace: "orders", Cell: "cell-1", Incarnation: incarnation, Shard: shardA, Epoch: e}
}

// acquire acquires the fence of the shard of the tests at the epoch e in s, and fails t
// when the acquisition does not end at once without an error.
func acquire(t *testing.T, s *machinetest.FencedStore, e epoch.Epoch) {
	t.Helper()
	done, err := s.Acquire(token(e)).Result()
	assert.True(t, done, "the acquisition ends at once")
	assert.NoError(t, err, "the acquisition raises the watermark")
}

// begin begins a write transaction of the tenure at the epoch e in s, and fails t when the
// transaction does not begin.
func begin(t *testing.T, s *machinetest.FencedStore, e epoch.Epoch) *machinetest.Txn {
	t.Helper()
	tx, err := s.Begin(token(e))
	assert.NoError(t, err, "the transaction begins")
	return tx
}

// fences runs core's fence laws over the write transactions of a FencedStore, each law on a
// store of its own.
func fences(t *testing.T) {
	t.Helper()
	epochtest.AssertFencedWriter(t, epochtest.FencedSystem[handle]{
		NewScope: func() epochtest.FencedScope[handle] {
			s := machinetest.NewFencedStore()
			return epochtest.FencedScope[handle]{
				Open: func(e epoch.Epoch) (handle, error) { return handle{s: s, t: token(e)}, nil },
				Supersede: func(e epoch.Epoch) {
					done, err := s.Acquire(token(e)).Result()
					expect.True(t, done, "the acquisition of the superseding tenure ends at once")
					expect.NoError(t, err, "the acquisition of the superseding tenure raises the watermark")
				},
				Snapshot: func() any { return s.Records() },
			}
		},
		Writes: map[string]func(handle) error{
			"applies the fence laws of core": func(h handle) error {
				tx, err := h.s.Begin(h.t)
				if err != nil {
					return fmt.Errorf("machinetest_test: begin at the epoch %d: %w", h.t.Epoch, err)
				}
				tx.Put(keyA, strconv.FormatUint(uint64(h.t.Epoch), 10))
				if err := tx.Commit(); err != nil {
					return fmt.Errorf("machinetest_test: commit at the epoch %d: %w", h.t.Epoch, err)
				}
				return nil
			},
		},
	})
}
