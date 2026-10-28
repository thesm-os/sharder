// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package memory

import (
	"cmp"
	"context"
	"fmt"
	"iter"
	"maps"
	"slices"
	"strings"
	"time"

	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/shard"
)

// eventKind is the kind of an event of a lease.
type eventKind uint8

// The kinds of the events of a lease.
const (
	// kindRenewal is a renewal of the lease, every Renew while the session renews.
	kindRenewal eventKind = 1
	// kindDeadline is the local deadline of the session: the validity of the lease
	// after the latest renewal that the store accepted.
	kindDeadline eventKind = 2
	// kindExpiry is the expiry of the lease at the store: the TTL after the latest
	// renewal that the store applied.
	kindExpiry eventKind = 3
)

// eventKey orders the events of the leases: by their due time, then by the order in
// which the store scheduled them.
type eventKey struct {
	due  time.Time
	seq  uint64
	kind eventKind
}

// compareEventKeys orders a and b by their due times, then by their sequence numbers.
func compareEventKeys(a, b eventKey) int {
	return cmp.Or(a.due.Compare(b.due), cmp.Compare(a.seq, b.seq))
}

// Membership is the membership.Membership of a Store, which keeps its sessions in the
// scopes of the store. Store.Membership returns it.
//
// # Concurrency
//
// A Membership is safe for concurrent use.
//
//dokimi:subject membership.Membership new=newBackedMembership
type Membership struct {
	s *Store
}

// The interfaces that a Membership implements.
var (
	_ membership.Membership = (*Membership)(nil)
	_ config.Provider       = (*Membership)(nil)
)

// Provides returns the capability leases.
func (*Membership) Provides() []config.Capability {
	return []config.Capability{config.Leases}
}

// Join registers n in the scope id in the namespace incarnation inc, as
// membership.Membership.Join states. The session renews its lease every l.Renew on the
// clock of the store, while ctx has not ended at the renewal. A Join of a node whose
// session is live ends that session, and gives the node a session of the next
// incarnation.
//
// Error modes:
//   - an error that wraps ErrClosed after Close.
//   - an error that wraps ErrOutage during an outage.
//   - shard.ErrIncarnation when a commit of another incarnation than inc created the
//     scope.
//   - an error of class errs.Invalid for a node without an ID, and an error that wraps
//     ownership.ErrConfig for a lease that ownership.Config.Validate refuses.
func (m *Membership) Join(ctx context.Context, id shard.Scope, inc shard.NamespaceIncarnation, n *shard.Node,
	l ownership.Lease,
) (membership.Session, error) {
	if n.ID == "" {
		return nil, fmt.Errorf("%w: join in %v", errNodeID, id)
	}
	if err := (ownership.Config{Lease: l, Fenced: true}).Validate(); err != nil {
		return nil, fmt.Errorf("memory: join of %s in %v: %w", n.ID, id, err)
	}
	s := m.s
	s.lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return nil, fmt.Errorf("memory: join of %s in %v: %w", n.ID, id, err)
	}
	sc := s.scope(id)
	if err := sc.admits(inc); err != nil {
		return nil, err
	}
	return sc.members.join(ctx, s, sc, inc, n, l), nil
}

// Watch yields the current view of the scope id in the incarnation inc, then every
// later view in the order of their revisions, as membership.Membership.Watch states.
// The nodes of a view are the nodes of the sessions of inc. The store keeps each view
// until every watch of the scope has read it or has ended.
//
// Error modes:
//   - an error that wraps ErrClosed after Close.
//   - an error that wraps ErrOutage during an outage.
//   - shard.ErrIncarnation when a commit of another incarnation than inc created the
//     scope, or creates it later.
func (m *Membership) Watch(ctx context.Context, id shard.Scope, inc shard.NamespaceIncarnation) iter.Seq2[
	membership.View, error,
] {
	return func(yield func(membership.View, error) bool) {
		r := &reader{}
		defer m.s.leave(id, r)
		for {
			views, changed, err := m.s.views(id, inc, r)
			if err != nil {
				yield(membership.View{}, err)
				return
			}
			for _, v := range views {
				if !yield(v, nil) {
					return
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-changed:
			}
		}
	}
}

// members is the membership of one scope. It contains the live sessions, the last
// incarnation of each node, and the views that the watchers of the scope have not read.
type members struct {
	// sessions maps each node with a live session to the session.
	sessions map[shard.NodeID]*session
	// last maps each node to the incarnation of its last session.
	last map[shard.NodeID]epoch.Epoch
	// readers lists the watchers of the views.
	readers map[*reader]struct{}
	// views lists the views that a watcher has not read, the oldest first, and the
	// current view.
	views []view
	// revision is the revision of the current view.
	revision epoch.Epoch
}

// newMembers returns the membership of a scope without sessions.
func newMembers() members {
	return members{
		sessions: make(map[shard.NodeID]*session),
		last:     make(map[shard.NodeID]epoch.Epoch),
		readers:  make(map[*reader]struct{}),
		views:    []view{{}},
	}
}

// join starts the session of n in sc for the incarnation inc, and records the view
// with the node. The caller has locked s.mu.
func (ms *members) join(ctx context.Context, s *Store, sc *scope, inc shard.NamespaceIncarnation, n *shard.Node,
	l ownership.Lease,
) *session {
	if old, ok := ms.sessions[n.ID]; ok {
		old.gone = true
		old.lose()
	}
	ms.last[n.ID]++
	node := *n.CloneKanon()
	node.Incarnation = ms.last[n.ID]
	now := s.clk.Time()
	ss := &session{
		s:        s,
		sc:       sc,
		ctx:      ctx,
		done:     make(chan struct{}),
		node:     node,
		inc:      inc,
		lease:    l,
		deadline: now.Add(l.Validity()),
		expiry:   now.Add(time.Duration(l.TTL)),
	}
	ms.sessions[n.ID] = ss
	s.schedule(ss, kindRenewal, now.Add(time.Duration(l.Renew)))
	s.schedule(ss, kindDeadline, ss.deadline)
	s.schedule(ss, kindExpiry, ss.expiry)
	ms.record(sc)
	return ss
}

// claim ends the sessions of every incarnation other than inc, after a commit of inc
// created the scope. The caller has locked the mutex of the store.
func (ms *members) claim(inc shard.NamespaceIncarnation) {
	for _, ss := range ms.sessions {
		if ss.inc != inc {
			ss.remove()
		}
	}
}

// end ends every session of the scope. The caller has locked the mutex of the store.
func (ms *members) end() {
	for _, ss := range ms.sessions {
		ss.remove()
	}
}

// record records the current view as the next revision, and wakes the watchers of sc.
// The caller has locked the mutex of the store.
func (ms *members) record(sc *scope) {
	ms.revision++
	nodes := slices.SortedFunc(maps.Values(ms.sessions), func(a, b *session) int {
		return strings.Compare(string(a.node.ID), string(b.node.ID))
	})
	ms.views = append(ms.views, view{sessions: nodes, revision: ms.revision})
	ms.trim()
	sc.notify()
}

// trim discards the views that every watcher has read, apart from the current view.
// The caller has locked the mutex of the store.
func (ms *members) trim() {
	oldest := ms.revision
	for r := range ms.readers {
		oldest = min(oldest, r.next)
	}
	i, _ := slices.BinarySearchFunc(ms.views, oldest, func(v view, rev epoch.Epoch) int {
		return cmp.Compare(v.revision, rev)
	})
	ms.views = slices.Clone(ms.views[i:])
}

// view is one revision of the membership of a scope: its live sessions in the order of
// their node IDs.
type view struct {
	sessions []*session
	revision epoch.Epoch
}

// at returns the view of id for the watchers of inc. It lists the nodes of the sessions
// of inc.
func (v *view) at(id shard.Scope, inc shard.NamespaceIncarnation) membership.View {
	mv := membership.View{Scope: id, Incarnation: inc, Revision: v.revision, Nodes: []shard.Node{}}
	for _, ss := range v.sessions {
		if ss.inc == inc {
			mv.Nodes = append(mv.Nodes, ss.node)
		}
	}
	return mv
}

// reader is a watcher of the views of a scope, with the revision of the next view that
// it reads. A reader without a revision reads the current view first.
type reader struct {
	next epoch.Epoch
}

// session is one session of one node in one scope.
type session struct {
	// expiry is the time at which the store expires the lease.
	expiry time.Time
	// deadline is the local deadline of the session.
	deadline time.Time
	// ctx is the context of the join. The session renews its lease while ctx has not
	// ended.
	ctx context.Context //nolint:containedctx // the context of Join bounds the renewals of the session.
	// done is closed when the session ends.
	done chan struct{}
	// s is the store of the scope.
	s *Store
	// sc is the scope of the session.
	sc *scope
	// node is the description of the node, with the incarnation of the session.
	node shard.Node
	// lease is the lease of the join.
	lease ownership.Lease
	// inc is the namespace incarnation of the join.
	inc shard.NamespaceIncarnation
	// closed reports that Done is closed. A session with a closed Done does not renew its
	// lease.
	closed bool
	// gone reports that the view no longer lists the node of the session.
	gone bool
}

// Incarnation returns the incarnation of the session.
func (ss *session) Incarnation() epoch.Epoch {
	return ss.node.Incarnation
}

// Done returns the channel that is closed when the session ends.
func (ss *session) Done() <-chan struct{} {
	return ss.done
}

// Leave ends the session and removes the node from the view at once. It returns nil for
// a session that has ended.
//
// Error modes:
//   - an error that wraps ErrClosed after Close.
//   - an error that wraps ErrOutage during an outage. The session then continues.
func (ss *session) Leave(context.Context) error {
	ss.s.lock()
	defer ss.s.mu.Unlock()
	if err := ss.s.check(); err != nil {
		return fmt.Errorf("memory: leave of %s in %v: %w", ss.node.ID, ss.sc.id, err)
	}
	ss.remove()
	return nil
}

// handle applies the event k of the session at its due time. A renewal applies while
// Done is open and the context of the join has not ended, and then schedules the next
// renewal, so the session has one renewal pending at a time. handle ignores a deadline
// or an expiry that a later renewal replaced. The caller has locked the mutex of the store.
func (ss *session) handle(k eventKey) {
	switch k.kind {
	case kindRenewal:
		if ss.closed || ss.ctx.Err() != nil {
			return
		}
		ss.renew(k.due)
		ss.s.schedule(ss, kindRenewal, k.due.Add(time.Duration(ss.lease.Renew)))
	case kindDeadline:
		if k.due.Equal(ss.deadline) {
			ss.lose()
		}
	case kindExpiry:
		if k.due.Equal(ss.expiry) {
			ss.remove()
		}
	}
}

// renew renews the lease at at. A renewal during an outage, or at a fault of
// PointRenewApply, fails. A renewal at a fault of PointRenewAck moves the expiry and
// not the deadline. The caller has locked the mutex of the store.
func (ss *session) renew(at time.Time) {
	if ss.s.outage || ss.s.failsAt(PointRenewApply) {
		return
	}
	ss.expiry = at.Add(time.Duration(ss.lease.TTL))
	ss.s.schedule(ss, kindExpiry, ss.expiry)
	if ss.s.failsAt(PointRenewAck) {
		return
	}
	ss.deadline = at.Add(ss.lease.Validity())
	ss.s.schedule(ss, kindDeadline, ss.deadline)
}

// lose ends the session at its node. It closes Done, and a session with a closed Done
// does not renew its lease. The caller has locked the mutex of the store.
func (ss *session) lose() {
	if !ss.closed {
		ss.closed = true
		close(ss.done)
	}
}

// remove removes the node of the session from the view, and ends the session. The
// caller has locked the mutex of the store.
func (ss *session) remove() {
	if ss.gone {
		return
	}
	ss.gone = true
	ss.lose()
	members := &ss.sc.members
	delete(members.sessions, ss.node.ID)
	members.record(ss.sc)
}

// Membership returns the membership of s, which keeps its sessions in the scopes of s.
func (s *Store) Membership() *Membership {
	return s.membership
}

// views returns the views of the scope id that the watcher r of the incarnation inc has
// not read, and the channel that the next change of the scope closes. It registers r
// with the scope.
func (s *Store) views(id shard.Scope, inc shard.NamespaceIncarnation, r *reader) (
	[]membership.View, <-chan struct{}, error,
) {
	s.lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return nil, nil, fmt.Errorf("memory: watch the members of %v: %w", id, err)
	}
	sc := s.scope(id)
	if err := sc.admits(inc); err != nil {
		return nil, nil, err
	}
	ms := &sc.members
	if _, ok := ms.readers[r]; !ok {
		ms.readers[r] = struct{}{}
		r.next = ms.revision
	}
	var views []membership.View
	for i := range ms.views {
		if v := &ms.views[i]; v.revision >= r.next {
			views = append(views, v.at(id, inc))
		}
	}
	r.next = ms.revision + 1
	ms.trim()
	return views, sc.changed, nil
}

// leave unregisters the watcher r of the scope id.
func (s *Store) leave(id shard.Scope, r *reader) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ms := &s.scope(id).members
	delete(ms.readers, r)
	ms.trim()
}
