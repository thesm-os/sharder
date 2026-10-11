// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package memory

import (
	"fmt"
	"math"
	"sync"
	"time"

	"go.thesmos.sh/core/btree"
	"go.thesmos.sh/core/clock"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// Store is an in-memory store.Store with hooks that inject faults. Its Membership keeps
// the sessions of nodes in the scopes of the store. The leases run on the clock that
// New receives, and every call first applies the renewals, deadlines and expiries that
// are due on that clock.
//
// The zero Store is not usable. Construct one with New.
//
// # Concurrency
//
// A Store is safe for concurrent use. One mutex orders every call, so the effects of a
// run depend only on the order of the calls and on the clock.
type Store struct {
	// clk is the clock of the leases.
	clk clock.Clock
	// scopes maps each scope that a call named to its state.
	scopes map[shard.Scope]*scope
	// armed maps each point to the number of faults that FailAt armed there and that no
	// call has used.
	armed map[Point]int
	// queue orders the renewals, deadlines and expiries of the leases by their due
	// time, then by the order in which the store scheduled them.
	queue *btree.MapFunc[eventKey, *session]
	// wake is the channel on which schedule wakes the goroutine of apply. A new event can
	// be due before the timer of that goroutine.
	wake chan struct{}
	// closed is closed by Close.
	closed chan struct{}
	// membership is the membership that Membership returns.
	membership *Membership
	// limits bounds every commit.
	limits store.Limits
	mu     sync.Mutex
	// seq numbers the scheduled events.
	seq uint64
	// outage reports an outage that StartOutage began.
	outage bool
	// done reports a call of Close.
	done bool
}

// The interfaces that a Store implements.
var (
	_ store.Store     = (*Store)(nil)
	_ config.Provider = (*Store)(nil)
)

// Option configures a Store.
type Option func(*options)

// options is the result of the Options that New receives.
type options struct {
	limits store.Limits
}

// WithLimits sets the bounds of every commit, which Limits returns and Fits applies.
// By default a Store has no bound, so every mutation fits.
func WithLimits(l store.Limits) Option {
	return func(o *options) { o.limits = l }
}

// New returns an empty store whose leases run on clk. It starts the goroutine that
// applies the events of the leases when they are due, and Close makes it return. The
// goroutine keeps at most one timer on clk, and stops each timer that it creates.
func New(clk clock.Clock, opts ...Option) *Store {
	o := options{limits: store.Limits{MaxPlacements: math.MaxInt, MaxBytes: math.MaxInt}}
	for _, opt := range opts {
		opt(&o)
	}
	s := &Store{
		clk:    clk,
		scopes: make(map[shard.Scope]*scope),
		armed:  make(map[Point]int),
		queue:  btree.NewMapFunc[eventKey, *session](compareEventKeys),
		wake:   make(chan struct{}, 1),
		closed: make(chan struct{}),
		limits: o.limits,
	}
	s.membership = &Membership{s: s}
	go s.apply()
	return s
}

// Provides returns the capability leases. A Store does not keep anything beyond its
// process, so it does not provide the capability durable.
func (*Store) Provides() []config.Capability {
	return []config.Capability{config.Leases}
}

// Limits returns the bounds of every commit, which WithLimits sets.
func (s *Store) Limits(shard.Scope) store.Limits {
	return s.limits
}

// Fits returns nil when one commit can apply m, and an error that wraps
// store.ErrTooLarge when m puts more placements than MaxPlacements or when its
// encoding is longer than MaxBytes.
func (s *Store) Fits(_ shard.Scope, m *store.Mutation) error {
	if len(m.Put) > s.limits.MaxPlacements {
		return fmt.Errorf("%w: %d placements, at most %d", store.ErrTooLarge, len(m.Put), s.limits.MaxPlacements)
	}
	if size := m.SizeKanon(); size > s.limits.MaxBytes {
		return fmt.Errorf("%w: %d bytes, at most %d", store.ErrTooLarge, size, s.limits.MaxBytes)
	}
	return nil
}

// Close ends every session, term and watch of the store, and makes the goroutine that
// New started return. Every later call returns an error that wraps ErrClosed. A second
// Close returns nil.
func (s *Store) Close() error {
	s.lock()
	defer s.mu.Unlock()
	if !s.done {
		s.done = true
		for _, sc := range s.scopes {
			sc.end()
		}
		close(s.closed)
	}
	return nil
}

// apply runs on the goroutine that New starts. It applies the events that are due,
// waits on a timer of the clock until the next one is due, and returns once Close has
// closed s.closed. It stops each timer after the wait.
func (s *Store) apply() {
	for {
		s.lock()
		next, _, ok := s.queue.Min()
		s.mu.Unlock()
		var tick <-chan time.Time
		var timer clock.Timer
		if ok {
			timer = s.clk.NewTimer(next.due.Sub(s.clk.Time()))
			tick = timer.C()
		}
		select {
		case <-tick:
		case <-s.wake:
		case <-s.closed:
		}
		if timer != nil {
			timer.Stop()
		}
		select {
		case <-s.closed:
			return
		default:
		}
	}
}

// lock locks s.mu, and applies the events that are due on the clock before the call
// that locks it. Every call of the store and of its Membership starts with lock.
func (s *Store) lock() {
	s.mu.Lock()
	s.pump()
}

// pump applies every event that is due on the clock, in the order of their due times,
// and the events of one due time in the order in which the store scheduled them. Each
// event takes effect at its due time, so the effects of a run do not depend on when the
// store applies them. The caller has locked s.mu.
func (s *Store) pump() {
	now := s.clk.Time()
	for {
		k, ss, ok := s.queue.Min()
		if !ok || k.due.After(now) {
			return
		}
		s.queue.Delete(k)
		ss.handle(k)
	}
}

// schedule adds the event of kind for ss at due, and wakes the goroutine of apply. The
// timer of that goroutine can wait for a later event. The caller has locked s.mu.
func (s *Store) schedule(ss *session, kind eventKind, due time.Time) {
	s.seq++
	s.queue.Set(eventKey{due: due, seq: s.seq, kind: kind}, ss)
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// check returns the error of a call to a closed store or during an outage. The caller
// has locked s.mu.
func (s *Store) check() error {
	switch {
	case s.done:
		return ErrClosed
	case s.outage:
		return ErrOutage
	default:
		return nil
	}
}

// scope returns the state of the scope id, and creates the empty state of a scope that
// no call named before. The caller has locked s.mu.
func (s *Store) scope(id shard.Scope) *scope {
	sc, ok := s.scopes[id]
	if !ok {
		sc = newScope(id)
		s.scopes[id] = sc
	}
	return sc
}

// failsAt reports whether the call fails at p. It fails when FailAt armed a fault of p,
// and then disarms one fault of p. The caller has locked s.mu.
func (s *Store) failsAt(p Point) bool {
	if s.armed[p] == 0 {
		return false
	}
	s.armed[p]--
	return true
}
