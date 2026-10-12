// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machine

import (
	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/shard"
)

// Join is the registration of a node in a scope through membership.Membership.Join. The
// runtime passes the result as a [Joined]. It passes a [SessionEnded] once the session that
// the join started ends.
type Join struct {
	// Node is the node to register.
	Node *shard.Node
	// Scope is the root or the cell that the node joins.
	Scope shard.Scope
	// Token is the token of the effect.
	Token Token
	// Lease is the lease of the session.
	Lease ownership.Lease
	// Incarnation is the namespace incarnation of the join.
	Incarnation shard.NamespaceIncarnation
}

var _ Effect = Join{}

// effect marks Join as an Effect.
func (Join) effect() {}

// Joined is the result of a [Join]. A Joined with an error is the last event of its join.
type Joined struct {
	// Err is the error of membership.Membership.Join, or nil.
	Err error
	// Token is the token of the Join.
	Token Token
	// Incarnation is the value of membership.Session.Incarnation for the session. The views
	// of the scope list the node with this incarnation.
	Incarnation epoch.Epoch
}

var _ Event = Joined{}

// event marks Joined as an Event.
func (Joined) event() {}

// SessionEnded is the end of the session that a [Join] started. The runtime passes it once
// the channel of membership.Session.Done is closed, such as after a [Leave]. It is the last
// event of its join.
type SessionEnded struct {
	// Token is the token of the Join.
	Token Token
}

var _ Event = SessionEnded{}

// event marks SessionEnded as an Event.
func (SessionEnded) event() {}

// Leave is the end of the session of a [Join]. The runtime ends the session through
// membership.Session.Leave. It then cancels the context of the join, so a session whose
// Leave failed still ends at its deadline. The runtime passes a [SessionEnded] for the
// session. For a join that stops without a session, it passes a [Joined] with an error.
// Leave has no result of its own.
type Leave struct {
	// Token is the token of the Join.
	Token Token
}

var _ Effect = Leave{}

// effect marks Leave as an Effect.
func (Leave) effect() {}

// WatchViews is a watch of the views of a scope through membership.Membership.Watch. The
// runtime passes each item of the watch as a [Viewed], until an error or an [Unwatch] ends
// the watch.
type WatchViews struct {
	// Scope is the scope of the views.
	Scope shard.Scope
	// Token is the token of the effect.
	Token Token
	// Incarnation is the namespace incarnation of the views.
	Incarnation shard.NamespaceIncarnation
}

var _ Effect = WatchViews{}

// effect marks WatchViews as an Effect.
func (WatchViews) effect() {}

// Viewed is one item of a [WatchViews]. Each item contains a view of the scope, until an
// item with an error ends the watch.
type Viewed struct {
	// Err is the error that ends the watch, or nil.
	Err error
	// View is the view that the watch yielded.
	View membership.View
	// Token is the token of the WatchViews.
	Token Token
}

var _ Event = Viewed{}

// event marks Viewed as an Event.
func (Viewed) event() {}
