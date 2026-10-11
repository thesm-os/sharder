// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package store

import (
	"context"
	"iter"
	"time"

	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/page"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/shard"
)

// Schema is the version of the record formats and of the scripts of the handoff
// protocols that this release writes and reads. A release that changes either raises
// it. A controller refuses to acquire a scope whose Head.Schema is above it, before it
// commits anything to the scope.
const Schema uint32 = 1

// State is the committed state of a scope apart from its placements, at one
// generation of one namespace incarnation.
//
// The zero State is the state of a scope before its first commit.
//
// # Concurrency
//
// A copy of a State shares its map, its slices and the bytes of Spec with the
// original. A State that Read returns is never modified.
type State struct {
	// Applied maps the ID of each applied proposal to its Expires. An ID that Applied
	// contains proves that the proposal applied. An ID that it does not contain does
	// not prove that the proposal was rejected, because a commit forgets each ID once
	// its Expires has passed.
	Applied map[proposal.ID]time.Time
	// Spec is the accepted spec as JSON, resolved with every default, in the root
	// scope only.
	Spec []byte
	// Transitions lists the transitions in flight, in the order of their shards.
	Transitions []Transition
	// Reservations lists the disruptions that the budget deferred or admitted and that
	// have not ended, in the order of their IDs. A reservation whose Admitted is zero is
	// pending.
	Reservations []budget.Reservation
	// Drains lists every drain, in the order of their nodes and then their disruption
	// IDs. A node with at least one drain does not receive new replicas.
	Drains []Drain
	// Mapping is the mapping of the namespace, in the root scope only.
	Mapping shard.Mapping
	// Cell is the record that the controller of the scope commits when it acquires
	// the scope. In the scope of a cell it is the record of the cell. In the root scope it
	// is the record of the leader of the root, with an empty ID.
	Cell shard.Cell
	// Head is the header of the scope.
	Head Head
}

// Query selects the placements that Read returns.
//
// The zero Query selects every placement of the scope, in position order.
type Query struct {
	// Node selects the placements with a replica on Node, in position order. The empty
	// Node selects every placement.
	Node shard.NodeID
}

// Event is one committed generation of a scope, or a report of progress when the
// generation does not change anything that concerns the watcher. Watch yields a report of
// progress at Generation only to a watcher that a node filters, when no change up to
// Generation concerns that watcher.
//
// # Concurrency
//
// A copy of an Event shares its slices with the original. An Event that Watch yields
// is never modified.
type Event struct {
	// Removed lists, for a watcher that a node filters, each shard whose placement
	// stops naming the node in this generation. A move to another node replaces the
	// placement, so Removed is the only record that the node lost the replica. Removed
	// is empty for every other watcher.
	Removed []shard.ID
	// Mutation is the mutation of the commit. For a watcher that a node filters, it
	// contains the ID of the commit and the changes that concern the node.
	Mutation Mutation
	// Generation is the generation that the commit created.
	Generation epoch.Epoch
}

// Limits bounds what one commit to a scope may contain. The planner splits a larger
// plan across consecutive generations, each a valid assignment.
//
// # Concurrency
//
// A Limits is a comparable value, and a copy is independent of the original.
type Limits struct {
	// MaxPlacements is the most placements that one commit may put, together with
	// their transitions and the indexes that the store keeps for them.
	MaxPlacements int
	// MaxBytes is the most bytes of the transaction that the store sends for one
	// commit, its keys, indexes, comparisons and receipt included.
	MaxBytes int
}

// Reader reads the scopes of namespaces. The agent, the router and sharderctl take a
// Reader, so they cannot commit.
//
// # Concurrency
//
// A Reader is safe for concurrent use.
type Reader interface {
	// Head returns the header of s with a linearizable read. A controller reads it to
	// learn the generation that its acquisition expects, and decides nothing else from
	// it. The Head of a scope before its first commit is zero.
	Head(ctx context.Context, s shard.Scope) (Head, error)
	// Read returns the state of s and a cursor over the placements that q selects, one
	// page per call. The read of the state is linearizable, and every page is at the
	// generation of the state. inc is the namespace incarnation that the reader
	// expects, or zero for a reader that has not learned it yet. Read returns the zero
	// State and an empty page for a scope before its first commit.
	//
	// Error modes:
	//   - shard.ErrIncarnation for a scope that a commit of another incarnation than a
	//     non-zero inc created, also for a later page of a scope that was deleted and
	//     created again by another incarnation.
	//   - ErrCompacted for a later page whose generation the store no longer keeps.
	Read(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation, q Query, p page.Page) (
		State, page.Cursor[shard.Placement], error)
	// Watch yields the generations of s later than from, in order and each whole,
	// until ctx ends. With an empty node, it yields every generation with its whole
	// mutation. With a node, it yields the changes of each generation that concern
	// node, and an empty event as progress for a generation without such a change, at
	// an interval that the implementation documents. A change concerns node when it
	// puts a placement that names node or named it before, creates, advances or ends a
	// transition with an endpoint on node, or replaces the controller record.
	//
	// Error modes:
	//   - ErrCompacted when from is older than the history of the store, or when the
	//     buffer of the watcher overflows. The iteration then stops.
	//   - shard.ErrIncarnation when a commit of another incarnation than a non-zero
	//     inc created the scope, or creates it again later. The iteration then stops.
	Watch(
		ctx context.Context,
		s shard.Scope,
		inc shard.NamespaceIncarnation,
		from epoch.Epoch,
		node shard.NodeID,
	) iter.Seq2[Event, error]
}

// Writer commits to one scope under one namespace incarnation and one fence epoch,
// which Store.Writer binds.
//
// # Concurrency
//
// A Writer is safe for concurrent use.
type Writer interface {
	// Commit applies m as the generation after expect, and writes the receipt of m in
	// the same transaction. It returns the generation that m created. Commit does not
	// modify m, and does not keep it after it returns.
	//
	// Commit returns ErrTooLarge, or the error of Mutation.Digest, before it sends
	// anything. In one atomic step with the mutation, it then checks the incarnation of
	// the scope, the fence, the receipt of the ID of m and the generation, in that order,
	// and returns the error of the first check that fails. A receipt of the ID of m with
	// the digest of m returns the recorded generation and does not apply m again. A commit
	// at generation zero creates the scope with the incarnation of the writer. An
	// admitted commit advances the watermark of the scope to the epoch of the writer, and
	// a commit that advances the watermark deletes the receipts of the terms below it.
	//
	// Error modes:
	//   - ErrTooLarge for an m that Fits refuses.
	//   - the error of Mutation.Digest for an m that does not encode.
	//   - shard.ErrIncarnation when a commit of another incarnation created the scope.
	//   - epoch.ErrFenced when the watermark of the scope is above the epoch of the
	//     writer.
	//   - ErrReused when the scope has recorded the ID of m with another digest.
	//   - version.ErrMismatch when the scope is not at expect.
	//   - any other error wraps version.ErrOutcomeUnknown: m may have applied.
	//
	// The first six errors are definite rejections. A definite rejection does not apply
	// anything.
	Commit(ctx context.Context, expect epoch.Epoch, m *Mutation) (epoch.Epoch, error)
}

// Leadership is one term of the election of a scope. It lasts until Done is closed.
//
// # Concurrency
//
// A Leadership is safe for concurrent use.
type Leadership interface {
	// Term returns the fence epoch of the term. The store grants each term of an
	// election to one candidate, above the watermark of the scope.
	Term() epoch.Epoch
	// Done is closed when the term ends. A term ends after Resign, when the context of
	// the Campaign that granted the term ends, or when the store ends the term.
	Done() <-chan struct{}
	// Resign ends the term, and lets the next candidate lead.
	Resign(ctx context.Context) error
}

// Store persists namespaces, one scope at a time. Each scope keeps one watermark, and
// the store applies core's fence laws to every commit in the same transaction as the
// commit.
//
// # Concurrency
//
// A Store is safe for concurrent use.
type Store interface {
	Reader
	// Writer returns a writer for s, bound to the namespace incarnation inc and the
	// epoch term. It does not perform I/O.
	Writer(s shard.Scope, inc shard.NamespaceIncarnation, term epoch.Epoch) Writer
	// Outcome returns the receipt of commit id, and reports whether s has one. It does
	// not apply anything. An absent receipt does not prove that the commit was rejected.
	Outcome(ctx context.Context, s shard.Scope, id CommitID) (Receipt, bool, error)
	// Campaign blocks until the caller leads the election of s, or until ctx ends. The
	// term lasts until Resign, until ctx ends, or until the store ends it. The root of
	// a namespace campaigns in the root scope.
	Campaign(ctx context.Context, s shard.Scope) (Leadership, error)
	// Limits returns the bounds of one commit to s.
	Limits(s shard.Scope) Limits
	// Fits returns nil when one commit to s can apply m, and ErrTooLarge otherwise. It
	// computes the size of the transaction that the store would send for m, and does
	// not perform I/O.
	Fits(s shard.Scope, m *Mutation) error
	// Close releases the resources of the store, and ends its watches and terms.
	Close() error
}
