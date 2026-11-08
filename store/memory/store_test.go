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

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/streamtest"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
	"go.thesmos.sh/sharder/store/storetest"
)

// limits are the limits of the stores of the laws. The law of Fits generates mutations on
// both sides of them, and every other law commits mutations below them.
var limits = store.Limits{MaxPlacements: 8, MaxBytes: 4096}

// commitPoints maps each loss of the suite of store.Store to the point that loses a commit
// there.
var commitPoints = map[storetest.Loss]memory.Point{
	storetest.LoseRequest: memory.PointCommitApply,
	storetest.LoseReply:   memory.PointCommitAck,
}

// faultyStore is a Store that loses the commits of the losses of the suite of store.Store.
// It deletes scopes and compacts its generations as a Store does.
type faultyStore struct {
	*memory.Store
}

// newFaultyStore returns a faultyStore over an empty Store with the limits of the tests, the
// subject of the suite of store.Store, and closes it when the check of h ends.
func newFaultyStore(h *storetest.StoreHandle) faultyStore {
	s := faultyStore{Store: memory.New(fake.New(origin), memory.WithLimits(limits))}
	h.Cleanup(func() { assert.NoError(h, s.Close(), "Close returns nil") })
	return s
}

// LoseCommit arms the fault of the store at the point of l.
func (s faultyStore) LoseCommit(l storetest.Loss) {
	s.FailAt(commitPoints[l])
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

// TestStore checks the construction, the capabilities, the limits and the closing of a
// Store.
func TestStore(t *testing.T) {
	t.Parallel()

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

	t.Run("WithManualTime", func(t *testing.T) {
		t.Parallel()

		t.Run("stops the store from starting a goroutine", func(t *testing.T) {
			t.Parallel()
			done := assert.NoGoroutineLeaks(t, "a store under WithManualTime starts no goroutine")
			s := memory.New(fake.New(origin), memory.WithManualTime())
			done()
			assert.NoError(t, s.Close(), "Close returns nil")
		})

		t.Run("stops the store from renewing a session", func(t *testing.T) {
			t.Parallel()
			clk := fake.New(origin)
			s := manual(t, clk)
			n := shard.Node{ID: node}
			ss, err := s.Membership().Join(t.Context(), scope, incA, &n, lease)
			assert.NoError(t, err, "Join registers the node")
			clk.Advance(time.Duration(lease.TTL))
			s.Pump()
			streamtest.Receive(t, ss.Done(), waitFor,
				"Done is closed at the TTL of the lease after the join without a renewal")
		})

		t.Run("leaves the local deadline of a session to the caller", func(t *testing.T) {
			t.Parallel()
			clk := fake.New(origin)
			s := manual(t, clk)
			n := shard.Node{ID: node}
			ss, err := s.Membership().Join(t.Context(), scope, incA, &n, lease)
			assert.NoError(t, err, "Join registers the node")
			clk.Advance(lease.Validity())
			s.Pump()
			streamtest.Blocks(t, ss.Done(), "Done is open at the validity of the lease after the join")
		})
	})

	t.Run("Pump", func(t *testing.T) {
		t.Parallel()

		t.Run("applies the expiry of a lease that is due on the clock", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				clk := fake.New(origin)
				s := manual(t, clk)
				n := shard.Node{ID: node}
				ss, err := s.Membership().Join(t.Context(), scope, incA, &n, lease)
				assert.NoError(t, err, "Join registers the node")
				clk.Advance(time.Duration(lease.TTL))
				synctest.Wait()
				streamtest.Blocks(t, ss.Done(), "Done is open until a call applies the expiry of the lease")
				s.Pump()
				streamtest.Receive(t, ss.Done(), waitFor, "Done is closed once Pump applies the expiry of the lease")
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
