// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package fencedstore

import (
	"context"
	"fmt"
	"maps"
	"sync"

	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// Reader reads the committed values of a store or of a replica.
type Reader interface {
	// Get returns the value of key, and reports whether key has one.
	Get(key string) (string, bool)
}

// fenceKey is the fence key of a token: its namespace incarnation and its shard.
type fenceKey struct {
	// incarnation is the namespace incarnation of the shard.
	incarnation shard.NamespaceIncarnation
	// shard is the shard.
	shard shard.ID
}

// fenceState is the fence of one fence key.
type fenceState struct {
	// watermark is the highest epoch that an acquisition or a write has set.
	watermark epoch.Epoch
	// acquired is the position of the store after the last acquisition of the key.
	acquired int
}

// Store is an in-memory key-value store whose transactions check the fence of each shard.
// Position n of the store is the state after its first n transactions, an acquisition
// included.
//
// The zero Store is not usable. Construct one with [New].
//
// # Concurrency
//
// A Store is safe for concurrent use. Its transactions and its acquisitions run one at a
// time.
type Store struct {
	// values maps each key to its committed value.
	values map[string]string
	// fences maps the fence key of each shard to its fence.
	fences map[fenceKey]fenceState
	// recorded contains the key of each callback that a committed transaction recorded.
	recorded map[protocol.Key]struct{}
	// log lists the writes of each committed transaction in order. An acquisition has no
	// writes.
	log []map[string]string
	// mu orders the transactions and the acquisitions.
	mu sync.Mutex
}

// The interfaces that a Store implements.
var (
	_ fence.Guard[*Tx] = (*Store)(nil)
	_ Reader           = (*Store)(nil)
)

// New returns an empty store.
func New() *Store {
	return &Store{
		values:   make(map[string]string),
		fences:   make(map[fenceKey]fenceState),
		recorded: make(map[protocol.Key]struct{}),
	}
}

// Acquire advances the watermark of the fence key of t to t.Epoch in one transaction, the
// acquisition of the tenure of t. A token at epoch.Zero is unfenced, so its acquisition leaves
// the watermark unchanged.
//
// Error modes:
//   - an error that wraps epoch.ErrFenced when the watermark is above t.Epoch.
//   - an error that wraps the cause of ctx when ctx has ended.
//
//nolint:gocritic // fence.Acquirer declares the token by value.
func (s *Store) Acquire(ctx context.Context, t fence.Token) error {
	if ctx.Err() != nil {
		return fmt.Errorf("fencedstore: the acquisition of shard %v: %w", t.Shard, context.Cause(ctx))
	}
	if t.Epoch == epoch.Zero {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := fenceKey{incarnation: t.Incarnation, shard: t.Shard}
	if w := s.fences[k].watermark; w > t.Epoch {
		return fmt.Errorf("fencedstore: the acquisition of shard %v at epoch %d below %d: %w",
			t.Shard, t.Epoch, w, epoch.ErrFenced)
	}
	s.log = append(s.log, nil)
	s.fences[k] = fenceState{watermark: t.Epoch, acquired: len(s.log)}
	return nil
}

// Writer returns the writer of the tenure of t. It does not perform I/O.
//
//nolint:gocritic // fence.Guard declares the token by value.
func (*Store) Writer(t fence.Token) fence.Writer[*Tx] {
	return &writer{token: t}
}

// Update runs fn in a transaction, and commits the writes and the callback keys of the
// transaction when fn returns nil. It discards them when fn returns an error. Update locks s
// for the whole transaction, so fn does not call s or a replica of s.
//
// Error modes:
//   - the error of fn, such as an error that wraps epoch.ErrFenced from a writer of an
//     older tenure.
//   - an error that wraps the cause of ctx when ctx has ended.
func (s *Store) Update(ctx context.Context, fn func(tx *Tx) error) error {
	if ctx.Err() != nil {
		return fmt.Errorf("fencedstore: a transaction: %w", context.Cause(ctx))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx := &Tx{
		s:        s,
		writes:   make(map[string]string),
		fences:   make(map[fenceKey]epoch.Epoch),
		recorded: make(map[protocol.Key]struct{}),
	}
	if err := fn(tx); err != nil {
		return err
	}
	maps.Copy(s.values, tx.writes)
	maps.Copy(s.recorded, tx.recorded)
	for k, e := range tx.fences {
		f := s.fences[k]
		f.watermark = e
		s.fences[k] = f
	}
	s.log = append(s.log, tx.writes)
	return nil
}

// Get returns the committed value of key, and reports whether key has one. It reads in a
// transaction of its own, so it returns the value of the last committed transaction.
func (s *Store) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.values[key]
	return v, ok
}

// NewReplica returns a replica of s at position zero, which applies the committed
// transactions of s when it reads.
func (s *Store) NewReplica() *Replica {
	return &Replica{s: s, values: make(map[string]string)}
}

// Reader returns r when r has applied the acquisition of the tenure of t, and s otherwise. A
// handler that reads the data of the shard of t through the result reads only a state that
// contains the acquisition, and every fenced write before it.
func (s *Store) Reader(t *fence.Token, r *Replica) Reader {
	s.mu.Lock()
	acquired := s.fences[fenceKey{incarnation: t.Incarnation, shard: t.Shard}].acquired
	s.mu.Unlock()
	if r.Position() >= acquired {
		return r
	}
	return s
}
