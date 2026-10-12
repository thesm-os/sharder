// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package memory_test

import (
	"context"
	"math"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/clock"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/streamtest"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
	"go.thesmos.sh/sharder/store/storetest"
)

// The names of the records of the tests.
const (
	namespace shard.Namespace = "orders"
	cell      shard.CellID    = "cell-1"
	node      shard.NodeID    = "node-a"
	otherNode shard.NodeID    = "node-b"
)

// The first bytes of the identifiers of the tests, one per kind of identifier.
const (
	tagIncarnation byte = 0x1a
	tagShard       byte = 0x5d
	tagProposal    byte = 0x9c
	tagDisruption  byte = 0xd1
)

// caller is the identity that requests the drains and the disruptions, and submits the
// proposals, of the tests.
const caller shard.Identity = "spiffe://memory_test/operator"

// term is the term of the writers of the tests.
const term epoch.Epoch = 1

// The scope, the incarnations, the placements and the clock of the tests.
var (
	// scope is the scope of the tests.
	scope = shard.Scope{Namespace: namespace, Cell: cell}
	// incA is the namespace incarnation of the commits of the tests.
	incA = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: tagIncarnation, 15: 0x0a})}
	// incB is the namespace incarnation of the tests of another incarnation.
	incB = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: tagIncarnation, 15: 0x0b})}
	// placements lists the two placements of the tests, in position order.
	placements = []shard.Placement{
		{
			Shard:    shard.ID{ID128: id.ID128{0: tagShard, 15: 0x01}},
			Range:    shard.Range{End: []byte{0x80}},
			Replicas: []shard.Replica{{Node: node, Role: shard.RolePrimary}},
		},
		{
			Shard:    shard.ID{ID128: id.ID128{0: tagShard, 15: 0x02}},
			Range:    shard.Range{Start: []byte{0x80}},
			Replicas: []shard.Replica{{Node: node, Role: shard.RolePrimary}},
		},
	}
	// origin is the start of the clock of each store of the tests.
	origin = time.Date(2026, time.October, 11, 0, 0, 0, 0, time.UTC)
)

// limits are the limits of the stores of the laws. The law of Fits generates mutations on
// both sides of them, and every other law commits mutations below them.
var limits = store.Limits{MaxPlacements: 8, MaxBytes: 4096}

// commitPoints maps each loss of the laws of package store to the point that loses a commit
// there.
var commitPoints = map[storetest.Loss]memory.Point{
	storetest.LoseRequest: memory.PointCommitApply,
	storetest.LoseReply:   memory.PointCommitAck,
}

// tracked is a context that a test follows with a weak pointer, to learn whether the store
// keeps a reference to it.
type tracked struct {
	context.Context //nolint:containedctx // the test follows this context to learn whether the store releases it.
}

// timers is a fake clock that counts its live timers. A timer is live from its creation
// until its Stop.
//
// # Concurrency
//
// A timers is safe for concurrent use.
type timers struct {
	*fake.Clock
	// mu guards live and most.
	mu sync.Mutex
	// live counts the timers that the clock created and whose Stop has not run.
	live int
	// most is the highest value of live.
	most int
}

// NewTimer returns a timer of the fake clock that counts its Stop, and counts it as live.
func (c *timers) NewTimer(d time.Duration) clock.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.live++
	c.most = max(c.most, c.live)
	return &timer{Timer: c.Clock.NewTimer(d), c: c}
}

// timer is a timer of a timers clock, which counts its Stop.
type timer struct {
	clock.Timer
	// c is the clock that created the timer.
	c *timers
}

// Stop stops the timer, and counts it as no longer live.
func (t *timer) Stop() bool {
	t.c.mu.Lock()
	t.c.live--
	t.c.mu.Unlock()
	return t.Timer.Stop()
}

// peak returns the most timers of c that were live at once.
func (c *timers) peak() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.most
}

// TestStore checks the laws of package store, and the construction, the capabilities, the
// limits and the closing of a Store.
func TestStore(t *testing.T) {
	t.Parallel()

	t.Run("passes the laws of package store", func(t *testing.T) {
		t.Parallel()
		storetest.AssertStore(t,
			func(assert.TB) store.Store { return memory.New(fake.New(origin), memory.WithLimits(limits)) },
			storetest.WithLostCommits(func(tb assert.TB, s store.Store, l storetest.Loss) {
				memoryOf(tb, s).FailAt(commitPoints[l])
			}),
			storetest.WithDelete(func(tb assert.TB, s store.Store, sc shard.Scope) { memoryOf(tb, s).Delete(sc) }),
			storetest.WithCompact(func(tb assert.TB, s store.Store) { memoryOf(tb, s).Compact() }),
		)
	})

	t.Run("New", func(t *testing.T) {
		t.Parallel()

		t.Run("returns a store without limits by default", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			assert.Equal(t, s.Limits(scope), store.Limits{MaxPlacements: math.MaxInt, MaxBytes: math.MaxInt},
				"a store without WithLimits has the largest limits")
		})

		t.Run("returns a store that applies the events that are due without a call", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				clk := fake.New(origin)
				s := memory.New(clk)
				t.Cleanup(func() { assert.NoError(t, s.Close(), "Close returns nil") })
				synctest.Wait()
				ended, cancel := context.WithCancel(t.Context())
				cancel()
				n := shard.Node{ID: node}
				ss, err := s.Membership().Join(ended, scope, incA, &n, lease)
				assert.NoError(t, err, "Join registers the node")
				synctest.Wait()
				clk.Advance(time.Duration(lease.Renew))
				synctest.Wait()
				clk.Advance(lease.Validity() - time.Duration(lease.Renew))
				streamtest.Receive(t, ss.Done(), waitFor, "Done is closed at the deadline of the join without a call")
			})
		})

		t.Run("returns a store that keeps at most one timer on its clock", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				clk := &timers{Clock: fake.New(origin)}
				s := memory.New(clk)
				t.Cleanup(func() { assert.NoError(t, s.Close(), "Close returns nil") })
				n := shard.Node{ID: node}
				_, err := s.Membership().Join(t.Context(), scope, incA, &n, lease)
				assert.NoError(t, err, "Join registers the node")
				synctest.Wait()
				clk.Advance(time.Duration(lease.Renew))
				synctest.Wait()
				assert.Equal(t, clk.peak(), 1, "the store stops each timer before it creates the next")
			})
		})
	})

	t.Run("Provides", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the capability leases", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, open(t).Provides(), []config.Capability{config.Leases},
				"a store provides leases and not durability")
		})
	})

	t.Run("Limits", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the limits of WithLimits", func(t *testing.T) {
			t.Parallel()
			s := memory.New(fake.New(origin), memory.WithLimits(limits))
			t.Cleanup(func() { assert.NoError(t, s.Close(), "Close returns nil") })
			assert.Equal(t, s.Limits(scope), limits, "Limits returns the limits of WithLimits")
		})
	})

	t.Run("Fits", func(t *testing.T) {
		t.Parallel()

		m := store.Mutation{Put: placements}
		size := m.SizeKanon()

		t.Run("returns nil for a mutation at both limits", func(t *testing.T) {
			t.Parallel()
			s := memory.New(
				fake.New(origin),
				memory.WithLimits(store.Limits{MaxPlacements: len(placements), MaxBytes: size}),
			)
			t.Cleanup(func() { assert.NoError(t, s.Close(), "Close returns nil") })
			assert.NoError(t, s.Fits(scope, &m), "Fits returns nil for a mutation at the limits of the store")
		})

		tests := []struct {
			name   string
			limits store.Limits
		}{
			{
				name:   "returns ErrTooLarge for a mutation of one placement more than the limit",
				limits: store.Limits{MaxPlacements: len(placements) - 1, MaxBytes: size},
			},
			{
				name:   "returns ErrTooLarge for a mutation of one byte more than the limit",
				limits: store.Limits{MaxPlacements: len(placements), MaxBytes: size - 1},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				s := memory.New(fake.New(origin), memory.WithLimits(tt.limits))
				t.Cleanup(func() { assert.NoError(t, s.Close(), "Close returns nil") })
				assert.ErrorIs(
					t,
					s.Fits(scope, &m),
					store.ErrTooLarge,
					"Fits returns ErrTooLarge for a mutation beyond a limit",
				)
			})
		}
	})

	t.Run("Close", func(t *testing.T) {
		t.Parallel()

		t.Run("returns nil for a second call", func(t *testing.T) {
			t.Parallel()
			s := memory.New(fake.New(origin))
			assert.NoError(t, s.Close(), "Close returns nil")
			assert.NoError(t, s.Close(), "a second Close returns nil")
		})

		t.Run("makes the goroutine of the store return", func(t *testing.T) {
			t.Parallel()
			done := assert.NoGoroutineLeaks(t, "the goroutine that New starts returns after Close")
			s := memory.New(fake.New(origin))
			assert.NoError(t, s.Close(), "Close returns nil")
			done()
		})
	})
}

// commit commits m to the scope of s through a writer of inc at the generation of the scope,
// with the commit ID of that generation, and returns the generation that m created. It fails
// t when the commit does not apply.
func commit(t *testing.T, s *memory.Store, inc shard.NamespaceIncarnation, m *store.Mutation) epoch.Epoch {
	t.Helper()
	h, err := s.Head(t.Context(), scope)
	assert.NoError(t, err, "Head returns the header of the scope")
	m.ID = store.CommitID{Incarnation: inc, Term: term, Seq: uint64(h.Generation) + 1}
	g, err := s.Writer(scope, inc, term).Commit(t.Context(), h.Generation, m)
	assert.NoError(t, err, "Commit applies the mutation")
	return g
}

// memoryOf returns the memory store s, which the laws pass to an option, and fails tb when s
// is another store.
func memoryOf(tb assert.TB, s store.Store) *memory.Store {
	tb.Helper()
	m, ok := s.(*memory.Store)
	assert.True(tb, ok, "the laws pass the store that newStore returned")
	return m
}

// open returns a store without limits on a clock of its own, and closes it when t ends.
func open(t *testing.T) *memory.Store {
	t.Helper()
	s := memory.New(fake.New(origin))
	t.Cleanup(func() { assert.NoError(t, s.Close(), "Close returns nil") })
	return s
}
