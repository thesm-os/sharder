// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package memory

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"iter"
	"maps"
	"slices"
	"strings"
	"time"

	"go.thesmos.sh/core/btree"
	"go.thesmos.sh/core/crypto"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/idmap"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/version"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// defaultPageSize is the number of placements on a page of a Read without a limit.
const defaultPageSize = 1000

// scope is the state of one scope. It contains the header, the generations, the
// receipts, the election and the membership of the scope.
type scope struct {
	// receipts maps the ID of each commit whose receipt the scope keeps to the receipt.
	receipts map[store.CommitID]store.Receipt
	// starts maps each shard to the start of the range of its current placement. The start
	// is the key of the placement in the generations.
	starts *idmap.Map[shard.ID, []byte]
	// changed is closed at every change that a watcher of the scope reads, and replaced
	// by a new channel.
	changed chan struct{}
	// id is the address of the scope.
	id shard.Scope
	// history lists the generations that the scope keeps, the oldest first. It is
	// empty for a scope before its first commit.
	history []*generation
	// members is the membership of the scope.
	members members
	// election is the election of the scope.
	election election
	// head is the header of the scope.
	head store.Head
}

// newScope returns the state of a scope before its first commit.
func newScope(id shard.Scope) *scope {
	return &scope{
		receipts: make(map[store.CommitID]store.Receipt),
		starts:   idmap.NewTyped128[shard.ID, []byte](0),
		changed:  make(chan struct{}),
		members:  newMembers(),
		id:       id,
	}
}

// admits returns nil for a scope before its first commit, or for a scope that a commit
// of the incarnation inc created. It returns an error that wraps shard.ErrIncarnation
// otherwise, also for a zero inc.
func (sc *scope) admits(inc shard.NamespaceIncarnation) error {
	if sc.head.Incarnation.IsZero() || sc.head.Incarnation == inc {
		return nil
	}
	return fmt.Errorf("%w: %v has the incarnation %v, not %v", shard.ErrIncarnation, sc.id, sc.head.Incarnation, inc)
}

// match returns nil for a zero inc, and what admits returns otherwise. A reader passes a
// zero inc until it has read the incarnation of the scope.
func (sc *scope) match(inc shard.NamespaceIncarnation) error {
	if inc.IsZero() {
		return nil
	}
	return sc.admits(inc)
}

// at returns the generation g of the scope, or nil when the scope does not keep it.
func (sc *scope) at(g epoch.Epoch) *generation {
	if len(sc.history) == 0 {
		return nil
	}
	oldest := sc.history[0].state.Head.Generation
	if g < oldest || g > sc.head.Generation {
		return nil
	}
	return sc.history[g-oldest]
}

// apply applies m as the next generation, and returns that generation. m is a copy of
// the mutation of a commit by w, and digest is the digest of m. A commit at generation
// zero creates the scope with the incarnation of w and ends the sessions of other
// incarnations. A commit that advances the watermark deletes the receipts of the terms
// below it. Each receipt of the scope is of a term at or below the old watermark, so
// such a commit deletes every receipt. The caller has locked the mutex of the store and
// has checked the commit.
func (sc *scope) apply(w *writer, m *store.Mutation, digest crypto.Digest) epoch.Epoch {
	//dokimi:mutate-skip ror-true: after the commit that creates the scope, the scope and each of its sessions have the incarnation of w, so the block does not change anything for a later commit
	if sc.head.Generation == 0 {
		sc.head.Incarnation = w.inc
		sc.members.claim(w.inc)
	}
	sc.head.Generation++
	sc.head.Schema = max(sc.head.Schema, store.Schema)
	if w.term > sc.head.Watermark {
		sc.head.Watermark = w.term
		clear(sc.receipts)
	}
	for _, id := range m.Confirm {
		delete(sc.receipts, id)
	}
	sc.receipts[m.ID] = store.Receipt{ID: m.ID, Digest: digest, Generation: sc.head.Generation}
	sc.history = append(sc.history, sc.next(m))
	sc.notify()
	return sc.head.Generation
}

// next returns the generation that m creates over the current generation of the
// scope, whose header the caller has already advanced. It records the start of each
// placement that m puts in sc.starts. The generation shares the applied proposals, the
// transitions, the reservations and the drains of the generation before it when m
// does not change them.
func (sc *scope) next(m *store.Mutation) *generation {
	g := &generation{
		placements: btree.NewMapFunc[[]byte, *shard.Placement](bytes.Compare),
		previous:   make([]shard.Placement, len(m.Put)),
		mutation:   *m,
	}
	if n := len(sc.history); n > 0 {
		g.placements = sc.history[n-1].placements.Clone()
		g.state = sc.history[n-1].state
	}
	for i := range m.Put {
		p := &m.Put[i]
		if start, ok := sc.starts.Get(p.Shard); ok {
			before, _ := g.placements.Delete(start)
			g.previous[i] = *before
		}
		g.placements.Set(p.Range.Start, p)
		sc.starts.Set(p.Shard, p.Range.Start)
	}
	g.state.Head = sc.head
	if m.Cell != nil {
		g.state.Cell = *m.Cell
	}
	if m.Mapping != nil {
		g.state.Mapping = *m.Mapping
	}
	if m.Spec != nil {
		g.state.Spec = m.Spec
	}
	if len(m.Applied) > 0 || len(m.Forget) > 0 {
		g.state.Applied = applied(g.state.Applied, m)
	}
	if len(m.Transitions) > 0 || len(m.Done) > 0 {
		g.state.Transitions, g.ended = transitions(g.state.Transitions, m)
	}
	if len(m.Reserve) > 0 || len(m.Release) > 0 {
		g.state.Reservations = reservations(g.state.Reservations, m)
	}
	if len(m.Drain) > 0 || len(m.Undrain) > 0 {
		g.state.Drains = drains(g.state.Drains, m)
	}
	return g
}

// notify wakes every watcher of the scope, which reads the scope again.
func (sc *scope) notify() {
	close(sc.changed)
	sc.changed = make(chan struct{})
}

// compact discards the generations of the scope below its current one. A watcher waits
// only for a generation after the current one, so compact wakes no watcher.
func (sc *scope) compact() {
	if n := len(sc.history); n > 0 {
		sc.history = slices.Clone(sc.history[n-1:])
	}
}

// end ends the sessions and the term of the scope, and wakes its watchers. Close and
// Delete call it.
func (sc *scope) end() {
	sc.members.end()
	sc.election.end()
	sc.notify()
}

// generation is one generation of a scope: the state after the commit that created it,
// the placements at the generation, and the changes of the commit that a watcher reads.
// The store never modifies a generation once the commit has created it.
type generation struct {
	// placements maps the start of the range of each placement to the placement, which is
	// an element of the Put of the mutation that created it.
	placements *btree.MapFunc[[]byte, *shard.Placement]
	// previous lists the placement that each placement of mutation.Put replaced, and
	// the zero Placement for a new shard.
	previous []shard.Placement
	// ended lists the transitions that mutation.Done ended.
	ended []store.Transition
	// mutation is a copy of the mutation of the commit.
	mutation store.Mutation
	// state is the state of the scope after the commit.
	state store.State
}

// event returns the event of the generation for a watcher that node filters, or for
// every watcher when node is empty. For a node, the event lists the changes that
// concern the node, and is an empty event as progress when none does.
func (g *generation) event(node shard.NodeID) store.Event {
	e := store.Event{Generation: g.state.Head.Generation}
	if node == "" {
		e.Mutation = g.mutation
		return e
	}
	m := store.Mutation{ID: g.mutation.ID, Cell: g.mutation.Cell}
	for i := range g.mutation.Put {
		p := &g.mutation.Put[i]
		now, before := names(p, node), names(&g.previous[i], node)
		if now || before {
			m.Put = append(m.Put, *p)
		}
		if before && !now {
			e.Removed = append(e.Removed, p.Shard)
		}
	}
	for i := range g.mutation.Transitions {
		if t := &g.mutation.Transitions[i]; involves(t, node) {
			m.Transitions = append(m.Transitions, *t)
		}
	}
	for i := range g.ended {
		if t := &g.ended[i]; involves(t, node) {
			m.Done = append(m.Done, t.Operation.Shard)
		}
	}
	if m.Cell != nil || len(m.Put) > 0 || len(m.Transitions) > 0 || len(m.Done) > 0 {
		e.Mutation = m
	}
	return e
}

// writer is the store.Writer of one scope, namespace incarnation and epoch.
type writer struct {
	s    *Store
	id   shard.Scope
	inc  shard.NamespaceIncarnation
	term epoch.Epoch
}

// Commit applies m as the generation after expect, as store.Writer.Commit states. A
// fault of PointCommitApply loses m before the checks, and a fault of PointCommitAck
// loses the reply after m applied. Commit does not modify m, and keeps a copy of it.
// The new generation shares the applied proposals, the transitions, the reservations
// and the drains of the generation before it when m does not change them.
//
// Error modes:
//   - an error that wraps store.ErrTooLarge for an m that Fits refuses.
//   - the error of Mutation.Digest for an m that does not encode.
//   - shard.ErrIncarnation, epoch.ErrFenced, store.ErrReused and
//     version.ErrMismatch, as store.Writer.Commit states.
//   - an error that wraps version.ErrOutcomeUnknown and ErrClosed, ErrOutage,
//     ErrFault or the error of ctx. The store returns it before the commit applies,
//     apart from ErrFault of PointCommitAck, which it returns after.
func (w *writer) Commit(ctx context.Context, expect epoch.Epoch, m *store.Mutation) (epoch.Epoch, error) {
	if err := w.s.Fits(w.id, m); err != nil {
		return 0, err
	}
	digest, err := m.Digest()
	if err != nil {
		return 0, fmt.Errorf("memory: commit %v to %v: %w", m.ID, w.id, err)
	}
	w.s.lock()
	defer w.s.mu.Unlock()
	if err := w.s.check(); err != nil {
		return 0, fmt.Errorf("memory: commit %v to %v: %w: %w", m.ID, w.id, version.ErrOutcomeUnknown, err)
	}
	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("memory: commit %v to %v: %w: %w", m.ID, w.id, version.ErrOutcomeUnknown, err)
	}
	if w.s.failsAt(PointCommitApply) {
		return 0, fmt.Errorf("memory: commit %v to %v: %w: %w", m.ID, w.id, version.ErrOutcomeUnknown, ErrFault)
	}
	sc := w.s.scope(w.id)
	if err := sc.admits(w.inc); err != nil {
		return 0, fmt.Errorf("memory: commit %v to %v: %w", m.ID, w.id, err)
	}
	if !epoch.Admissible(sc.head.Watermark, w.term) {
		return 0, fmt.Errorf("%w: commit %v to %v at the epoch %d below the watermark %d",
			epoch.ErrFenced, m.ID, w.id, w.term, sc.head.Watermark)
	}
	if r, ok := sc.receipts[m.ID]; ok {
		if r.Digest != digest {
			return 0, fmt.Errorf("%w: commit %v to %v", store.ErrReused, m.ID, w.id)
		}
		return r.Generation, nil
	}
	if sc.head.Generation != expect {
		return 0, fmt.Errorf("%w: commit %v to %v expects the generation %d at the generation %d",
			version.ErrMismatch, m.ID, w.id, expect, sc.head.Generation)
	}
	g := sc.apply(w, m.CloneKanon(), digest)
	if w.s.failsAt(PointCommitAck) {
		return 0, fmt.Errorf("memory: commit %v to %v: %w: %w", m.ID, w.id, version.ErrOutcomeUnknown, ErrFault)
	}
	return g, nil
}

// names reports whether p has a replica on node.
func names(p *shard.Placement, node shard.NodeID) bool {
	return slices.ContainsFunc(p.Replicas, func(r shard.Replica) bool { return r.Node == node })
}

// involves reports whether t has an endpoint on node.
func involves(t *store.Transition, node shard.NodeID) bool {
	return t.From.Node == node || t.To.Node == node || t.Fallback.Node == node
}

// applied returns the applied proposals of a state after m: prev with the proposals of
// m.Applied and without the IDs of m.Forget. It does not modify prev.
func applied(prev map[proposal.ID]time.Time, m *store.Mutation) map[proposal.ID]time.Time {
	next := maps.Clone(prev)
	if next == nil {
		next = make(map[proposal.ID]time.Time, len(m.Applied))
	}
	for i := range m.Applied {
		next[m.Applied[i].Proposal.ID] = m.Applied[i].Proposal.Expires
	}
	for _, id := range m.Forget {
		delete(next, id)
	}
	return next
}

// transitions returns the transitions of a state after m, in the order of their
// shards, and the transitions that m ends. m ends the transitions of m.Done first, and
// then creates or advances those of m.Transitions. It does not modify prev.
func transitions(prev []store.Transition, m *store.Mutation) ([]store.Transition, []store.Transition) {
	//dokimi:mutate-skip aor: the sum presizes the map, and Set grows a map of a smaller capacity
	byShard := idmap.NewTyped128[shard.ID, *store.Transition](len(prev) + len(m.Transitions))
	for i := range prev {
		byShard.Set(prev[i].Operation.Shard, &prev[i])
	}
	var ended []store.Transition
	for _, id := range m.Done {
		if t, ok := byShard.Delete(id); ok {
			ended = append(ended, *t)
		}
	}
	for i := range m.Transitions {
		byShard.Set(m.Transitions[i].Operation.Shard, &m.Transitions[i])
	}
	next := slices.Grow([]store.Transition(nil), byShard.Len())
	for _, t := range byShard.All() {
		next = append(next, *t)
	}
	slices.SortFunc(next, func(a, b store.Transition) int { return a.Operation.Shard.Compare(b.Operation.Shard) })
	return next, ended
}

// reservations returns the reservations of a state after m, in the order of their IDs:
// prev with the reservations of m.Reserve, which replace those with the same IDs, and
// without the IDs of m.Release. It does not modify prev.
func reservations(prev []budget.Reservation, m *store.Mutation) []budget.Reservation {
	byID := make(map[budget.ID]budget.Reservation, len(prev)+len(m.Reserve))
	for i := range prev {
		byID[prev[i].Disruption.ID] = prev[i]
	}
	for i := range m.Reserve {
		byID[m.Reserve[i].Disruption.ID] = m.Reserve[i]
	}
	for _, id := range m.Release {
		delete(byID, id)
	}
	return slices.SortedFunc(maps.Values(byID), func(a, b budget.Reservation) int {
		return a.Disruption.ID.Compare(b.Disruption.ID.ID)
	})
}

// drains returns the drains of a state after m, in the order of their nodes and then
// their disruption IDs: prev with the drains of m.Drain, which replace those with the
// same keys, and without the keys of m.Undrain. It does not modify prev.
func drains(prev []store.Drain, m *store.Mutation) []store.Drain {
	byKey := make(map[store.DrainKey]store.Drain, len(prev)+len(m.Drain))
	for _, d := range prev {
		byKey[store.DrainKey{Node: d.Node, Disruption: d.Disruption}] = d
	}
	for _, d := range m.Drain {
		byKey[store.DrainKey{Node: d.Node, Disruption: d.Disruption}] = d
	}
	for _, k := range m.Undrain {
		delete(byKey, k)
	}
	return slices.SortedFunc(maps.Values(byKey), func(a, b store.Drain) int {
		return cmp.Or(strings.Compare(string(a.Node), string(b.Node)), a.Disruption.Compare(b.Disruption.ID))
	})
}

// Head returns the header of the scope id. The Head of a scope that no commit has
// created is zero.
//
// Error modes:
//   - an error that wraps ErrClosed after Close.
//   - an error that wraps ErrOutage during an outage.
func (s *Store) Head(_ context.Context, id shard.Scope) (store.Head, error) {
	s.lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return store.Head{}, fmt.Errorf("memory: head of %v: %w", id, err)
	}
	return s.scope(id).head, nil
}

// Read returns the state of the scope id at its current generation and the first page
// of the placements that q selects, or the page after the token of p at the generation
// of the token. A page has p.Limit placements, or 1,000 when p states no limit. The
// state and the placements share memory with the store, so the caller does not modify
// them.
//
// Error modes:
//   - an error that wraps ErrClosed after Close.
//   - an error that wraps ErrOutage during an outage.
//   - an error that wraps ErrToken for a page token that Read did not write.
//   - shard.ErrIncarnation for a scope of another incarnation than a non-zero inc, or
//     than the incarnation of the token.
//   - store.ErrCompacted for a token whose generation the scope no longer keeps.
func (s *Store) Read(_ context.Context, id shard.Scope, inc shard.NamespaceIncarnation, q store.Query, p page.Page) (
	store.State, page.Cursor[shard.Placement], error,
) {
	s.lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return store.State{}, nil, fmt.Errorf("memory: read %v: %w", id, err)
	}
	sc := s.scope(id)
	if err := sc.match(inc); err != nil {
		return store.State{}, nil, err
	}
	g := sc.at(sc.head.Generation)
	var after []byte
	resume := p.Token != ""
	if resume {
		t, err := parseToken(p.Token)
		if err != nil {
			return store.State{}, nil, err
		}
		if err := sc.admits(t.incarnation); err != nil {
			return store.State{}, nil, err
		}
		if g = sc.at(t.generation); g == nil {
			return store.State{}, nil, fmt.Errorf("%w: the generation %d of %v", store.ErrCompacted, t.generation, id)
		}
		after = t.after
	}
	if g == nil {
		return store.State{}, page.NewSliceCursor[shard.Placement](nil, ""), nil
	}
	limit := p.WithDefault(defaultPageSize).Limit
	placements := make([]shard.Placement, 0, min(limit, g.placements.Len()))
	next := ""
	for start, pl := range g.placements.Ascend(after) {
		if resume && bytes.Equal(start, after) || q.Node != "" && !names(pl, q.Node) {
			continue
		}
		if len(placements) == limit {
			t := token{
				after:       placements[len(placements)-1].Range.Start,
				incarnation: g.state.Head.Incarnation,
				generation:  g.state.Head.Generation,
			}
			next = t.String()
			break
		}
		placements = append(placements, *pl)
	}
	return g.state, page.NewSliceCursor(placements, next), nil
}

// Watch yields the generations of the scope id after from, as store.Reader.Watch
// states. A watcher that a node filters receives an empty event as progress for every
// generation that does not concern the node. A watcher reads the generations that the
// scope keeps, so its watch ends with store.ErrCompacted only when Compact discards its
// next generation. Every event shares memory with the store, so the caller does not
// modify it.
//
// Error modes:
//   - an error that wraps ErrClosed after Close.
//   - an error that wraps ErrOutage during an outage.
//   - shard.ErrIncarnation and store.ErrCompacted, as store.Reader.Watch states.
func (s *Store) Watch(ctx context.Context, id shard.Scope, inc shard.NamespaceIncarnation, from epoch.Epoch,
	node shard.NodeID,
) iter.Seq2[store.Event, error] {
	return func(yield func(store.Event, error) bool) {
		next := from + 1
		for {
			events, changed, err := s.events(id, inc, next, node)
			if err != nil {
				yield(store.Event{}, err)
				return
			}
			for i := range events {
				if !yield(events[i], nil) {
					return
				}
				next = events[i].Generation + 1
			}
			select {
			case <-ctx.Done():
				return
			case <-changed:
			}
		}
	}
}

// Outcome returns the receipt of the commit id in the scope sc, and reports whether the
// scope keeps one.
//
// Error modes:
//   - an error that wraps ErrClosed after Close.
//   - an error that wraps ErrOutage during an outage.
func (s *Store) Outcome(_ context.Context, sc shard.Scope, id store.CommitID) (store.Receipt, bool, error) {
	s.lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return store.Receipt{}, false, fmt.Errorf("memory: outcome in %v: %w", sc, err)
	}
	r, ok := s.scope(sc).receipts[id]
	return r, ok, nil
}

// Writer returns the writer of the scope id for the namespace incarnation inc and the
// epoch term. It does not perform I/O.
func (s *Store) Writer(id shard.Scope, inc shard.NamespaceIncarnation, term epoch.Epoch) store.Writer {
	return &writer{s: s, id: id, inc: inc, term: term}
}

// Delete removes the scope id, as an operator deletes the keys of a namespace before
// the namespace is created again under the same name. It removes the generations, the
// receipts and the header of the scope. It also ends the sessions and the term of the
// scope, and wakes its watchers. A later commit at generation zero creates the scope
// again.
func (s *Store) Delete(id shard.Scope) {
	s.lock()
	defer s.mu.Unlock()
	sc, ok := s.scopes[id]
	if !ok {
		return
	}
	sc.end()
	sc.head = store.Head{}
	sc.history = nil
	clear(sc.receipts)
	sc.starts.Clear()
}

// events returns the events of the generations of the scope id from next on, for a
// watcher of the incarnation inc that node filters, and the channel that the next
// change of the scope closes.
func (s *Store) events(id shard.Scope, inc shard.NamespaceIncarnation, next epoch.Epoch, node shard.NodeID) (
	[]store.Event, <-chan struct{}, error,
) {
	s.lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return nil, nil, fmt.Errorf("memory: watch %v: %w", id, err)
	}
	sc := s.scope(id)
	if err := sc.match(inc); err != nil {
		return nil, nil, err
	}
	if next > sc.head.Generation {
		return nil, sc.changed, nil
	}
	oldest := sc.history[0].state.Head.Generation
	if next < oldest {
		return nil, nil, fmt.Errorf("%w: the generation %d of %v, the oldest kept is %d",
			store.ErrCompacted, next, id, oldest)
	}
	kept := sc.history[next-oldest:]
	events := make([]store.Event, 0, len(kept))
	for _, g := range kept {
		events = append(events, g.event(node))
	}
	return events, sc.changed, nil
}
