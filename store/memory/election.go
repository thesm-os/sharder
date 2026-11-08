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

// candidate is one call of Candidate that waits for a term.
type candidate struct {
	// ctx is the context of the call, which bounds the wait and the term.
	ctx context.Context //nolint:containedctx // the context of Candidate bounds the term that it grants.
	// sc is the scope of the election.
	sc *scope
	// granted is closed when the election grants a term to the candidate.
	granted chan struct{}
	// lead is the term that the election granted to the candidate, or nil.
	lead *leadership
}

// settled ends the term of c once the context of c has ended, and removes c from the
// candidates that wait when its context ends before the grant. It reports whether the store
// no longer checks c: once the term has ended, or once c has left the candidates. The
// caller has locked the mutex of the store.
func (c *candidate) settled() bool {
	if c.lead != nil {
		if c.ctx.Err() != nil {
			c.lead.end()
		}
		return c.lead.ended
	}
	if c.ctx.Err() == nil {
		return false
	}
	c.sc.election.waiting = slices.DeleteFunc(c.sc.election.waiting, func(w *candidate) bool { return w == c })
	return true
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
	// It is nil under WithManualTime, where each call of the store checks that context.
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
	if l.stop != nil {
		l.stop()
	}
	close(l.done)
	l.sc.election.leader = nil
	if !l.s.done {
		l.s.elect(l.sc)
	}
}

// Candidacy is one candidate of an election that waits for a term. Store.Candidate returns
// it.
//
// # Concurrency
//
// A Candidacy is safe for concurrent use.
type Candidacy struct {
	// s is the store of the election.
	s *Store
	// c is the candidate in the election.
	c *candidate
}

// Granted returns the channel that the election closes when it grants the term.
func (c *Candidacy) Granted() <-chan struct{} {
	return c.c.granted
}

// Lead returns the term that the election granted, and false before the grant.
func (c *Candidacy) Lead() (store.Leadership, bool) {
	c.s.lock()
	defer c.s.mu.Unlock()
	if c.c.lead == nil {
		return nil, false
	}
	return c.c.lead, true
}

// Campaign blocks until the caller leads the election of the scope id, or until ctx
// ends. It waits on the candidacy that Candidate returns, so the store grants the terms of
// Campaign and Candidate in the order of the calls, each one above the watermark of the
// scope and above every earlier term. A term lasts until Resign, until ctx ends, or until
// Close or Delete ends it. A call that the election grants a term as ctx ends returns the
// term, which ends at once. The store keeps a reference to ctx until the call returns
// without a term, or until the term ends.
//
// Error modes:
//   - an error that wraps ErrClosed after Close, also while the call waits.
//   - an error that wraps ErrOutage during an outage.
//   - an error that wraps the cause of ctx when ctx ends before the call leads.
func (s *Store) Campaign(ctx context.Context, id shard.Scope) (store.Leadership, error) {
	c, err := s.Candidate(ctx, id)
	if err != nil {
		return nil, err
	}
	select {
	case <-c.c.granted:
	case <-ctx.Done():
	case <-s.closed:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if c.c.lead != nil {
		return c.c.lead, nil
	}
	e := &c.c.sc.election
	e.waiting = slices.DeleteFunc(e.waiting, func(w *candidate) bool { return w == c.c })
	if s.done {
		return nil, fmt.Errorf("memory: campaign in %v: %w", id, ErrClosed)
	}
	return nil, fmt.Errorf("memory: campaign in %v: %w", id, context.Cause(ctx))
}

// Candidate enters the election of the scope id without waiting. The election grants the
// candidacy a term in the order of the calls, as it grants a term to Campaign. The term ends
// when ctx ends. Under WithManualTime the store ends the term at its first call after ctx
// ends, and at that call drops a candidacy whose ctx ended before the grant. The store keeps
// a reference to ctx until it drops the candidacy, or until the term ends.
//
// Error modes:
//   - an error that wraps ErrClosed after Close.
//   - an error that wraps ErrOutage during an outage.
func (s *Store) Candidate(ctx context.Context, id shard.Scope) (*Candidacy, error) {
	s.lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return nil, fmt.Errorf("memory: campaign in %v: %w", id, err)
	}
	sc := s.scope(id)
	c := &candidate{ctx: ctx, sc: sc, granted: make(chan struct{})}
	sc.election.waiting = append(sc.election.waiting, c)
	if s.manual {
		s.candidacies = append(s.candidacies, c)
	}
	s.elect(sc)
	return &Candidacy{s: s, c: c}, nil
}

// elect grants a term to the first candidate of the scope sc whose context has not
// ended, when the scope has no leader. The term is above the watermark of the scope and
// above every earlier term of the election. elect removes each candidate that it takes
// from the list of waiting candidates, so the list does not keep a reference to it.
// Outside WithManualTime, a callback ends the term once the context of the candidate
// ends. The caller has locked s.mu.
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
		if !s.manual {
			l.stop = context.AfterFunc(c.ctx, func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				l.end()
			})
		}
		e.leader = l
		c.lead = l
		close(c.granted)
	}
}
