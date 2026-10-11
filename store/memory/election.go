// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package memory

import (
	"context"
	"fmt"
	"slices"

	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// election is the election of one scope. It contains the term of its leader, and the
// candidates that wait for a term in the order of their calls of Campaign.
type election struct {
	// leader is the current term, or nil while the scope has no leader.
	leader *leadership
	// waiting lists the candidates that wait for a term, the earliest first.
	waiting []*candidate
	// last is the last term that the election granted.
	last epoch.Epoch
}

// end ends the term of the leader. The caller has locked the mutex of the store.
func (e *election) end() {
	if e.leader != nil {
		e.leader.end()
	}
}

// candidate is one call of Campaign that waits for a term.
type candidate struct {
	// ctx is the context of the call, which bounds the wait and the term.
	ctx context.Context //nolint:containedctx // the context of Campaign bounds the term that it grants.
	// granted is closed when the election grants a term to the candidate.
	granted chan struct{}
	// lead is the term that the election granted to the candidate, or nil.
	lead *leadership
}

// leadership is one term of the election of a scope.
type leadership struct {
	// s is the store of the scope.
	s *Store
	// sc is the scope of the election.
	sc *scope
	// done is closed when the term ends.
	done chan struct{}
	// stop cancels the callback that ends the term when the context of the campaign ends.
	stop func() bool
	// term is the fence epoch of the term.
	term epoch.Epoch
	// ended reports that the term has ended.
	ended bool
}

// Term returns the fence epoch of the term.
func (l *leadership) Term() epoch.Epoch {
	return l.term
}

// Done returns the channel that is closed when the term ends.
func (l *leadership) Done() <-chan struct{} {
	return l.done
}

// Resign ends the term, and lets the next candidate lead. It returns nil for a term that
// has ended.
//
// Error modes:
//   - an error that wraps ErrClosed after Close.
//   - an error that wraps ErrOutage during an outage. The term then continues.
func (l *leadership) Resign(context.Context) error {
	l.s.lock()
	defer l.s.mu.Unlock()
	if err := l.s.check(); err != nil {
		return fmt.Errorf("memory: resign the term %d of %v: %w", l.term, l.sc.id, err)
	}
	l.end()
	return nil
}

// end ends the term and releases the context of its campaign. It then lets the next
// candidate lead, unless the store is closed. A term that has not ended is the leader of
// its election. The caller has locked the mutex of the store.
func (l *leadership) end() {
	if l.ended {
		return
	}
	l.ended = true
	l.stop()
	close(l.done)
	l.sc.election.leader = nil
	if !l.s.done {
		l.s.elect(l.sc)
	}
}

// Campaign blocks until the caller leads the election of the scope id, or until ctx
// ends. The store grants the terms in the order of the calls, each one above the
// watermark of the scope and above every earlier term. A term lasts until Resign, until
// ctx ends, or until Close or Delete ends it. A call that the election grants a term as
// ctx ends returns the term, which ends at once. The store keeps a reference to ctx
// until the call returns without a term, or until the term ends.
//
// Error modes:
//   - an error that wraps ErrClosed after Close, also while the call waits.
//   - an error that wraps ErrOutage during an outage.
//   - an error that wraps the cause of ctx when ctx ends before the call leads.
func (s *Store) Campaign(ctx context.Context, id shard.Scope) (store.Leadership, error) {
	s.lock()
	if err := s.check(); err != nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("memory: campaign in %v: %w", id, err)
	}
	sc := s.scope(id)
	c := &candidate{ctx: ctx, granted: make(chan struct{})}
	sc.election.waiting = append(sc.election.waiting, c)
	s.elect(sc)
	s.mu.Unlock()
	select {
	case <-c.granted:
	case <-ctx.Done():
	case <-s.closed:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if c.lead != nil {
		return c.lead, nil
	}
	sc.election.waiting = slices.DeleteFunc(sc.election.waiting, func(w *candidate) bool { return w == c })
	if s.done {
		return nil, fmt.Errorf("memory: campaign in %v: %w", id, ErrClosed)
	}
	return nil, fmt.Errorf("memory: campaign in %v: %w", id, context.Cause(ctx))
}

// elect grants a term to the first candidate of the scope sc whose context has not
// ended, when the scope has no leader. The term is above the watermark of the scope and
// above every earlier term of the election. elect removes each candidate that it takes
// from the list of waiting candidates, so the list does not keep a reference to it. The
// caller has locked s.mu.
func (s *Store) elect(sc *scope) {
	e := &sc.election
	for e.leader == nil && len(e.waiting) > 0 {
		c := e.waiting[0]
		e.waiting = slices.Delete(e.waiting, 0, 1)
		if c.ctx.Err() != nil {
			continue
		}
		e.last = max(e.last, sc.head.Watermark) + 1
		l := &leadership{s: s, sc: sc, done: make(chan struct{}), term: e.last}
		l.stop = context.AfterFunc(c.ctx, func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			l.end()
		})
		e.leader = l
		c.lead = l
		close(c.granted)
	}
}
