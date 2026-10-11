// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package membership

import (
	"context"
	"iter"
	"slices"
	"strings"

	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/shard"
)

// View is one list of the live nodes of a scope in one namespace incarnation.
//
// The zero View has no nodes.
//
// # Concurrency
//
// A copy of a View shares the elements of Nodes with the original. A View that Watch
// yields is never modified.
type View struct {
	// Scope is the scope of the nodes: the root or one cell.
	Scope shard.Scope
	// Nodes lists the live nodes in the order of their IDs, each with the incarnation
	// of its session.
	Nodes []shard.Node
	// Revision increases with every change of the view.
	Revision epoch.Epoch
	// Incarnation is the namespace incarnation of the view.
	Incarnation shard.NamespaceIncarnation
}

// Node returns the node of v with the ID id, and reports whether v has one. It finds
// the node by binary search over the IDs of Nodes, so it compares O(log n) IDs for n
// nodes.
//
// # Allocation contract
//
// Zero alloc.
func (v *View) Node(id shard.NodeID) (shard.Node, bool) {
	i, ok := slices.BinarySearchFunc(v.Nodes, id, func(n shard.Node, id shard.NodeID) int {
		return strings.Compare(string(n.ID), string(id))
	})
	if !ok {
		return shard.Node{}, false
	}
	return v.Nodes[i], true
}

// Session is one incarnation of one node in one scope.
//
// # Concurrency
//
// A Session is safe for concurrent use.
type Session interface {
	// Incarnation returns the incarnation of the session. It is higher than the
	// incarnation of every earlier session of the same node in the same scope.
	Incarnation() epoch.Epoch
	// Done is closed when the lease of the session is lost: when the backend reports
	// the loss, or when the local deadline passes. The deadline is the send time of the
	// latest accepted renewal plus the validity of the lease, on a timer of the clock.
	// A renewal that fails, or whose reply does not arrive, does not move the deadline.
	// A reply that arrives after Done has closed does not reopen the session.
	Done() <-chan struct{}
	// Leave ends the session and removes the node from the view at once.
	Leave(ctx context.Context) error
}

// Membership registers nodes in scopes and reports which nodes are live. A node is
// live while the backend renews its lease.
//
// # Concurrency
//
// A Membership is safe for concurrent use.
type Membership interface {
	// Join registers n in s, the root or one cell, in the namespace incarnation inc.
	// It renews the lease every l.Renew, for a lease of l.TTL, until ctx ends or Leave
	// is called. Join does not modify n, and does not keep it after it returns.
	//
	// Error modes:
	//   - shard.ErrIncarnation when a commit of another incarnation than inc created
	//     s.
	//   - an error of class errs.Invalid for a node without an ID, and for a lease that
	//     ownership.Config.Validate refuses.
	Join(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation, n *shard.Node, l ownership.Lease) (
		Session, error)
	// Watch yields the current view of s in the namespace incarnation inc, then every
	// later view in the order of their revisions, until ctx ends.
	//
	// Error modes:
	//   - shard.ErrIncarnation when a commit of another incarnation than inc created
	//     s, or creates it later. The iteration then stops.
	Watch(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation) iter.Seq2[View, error]
}
