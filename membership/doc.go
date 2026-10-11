// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package membership tracks the live nodes of the scopes of namespaces.
//
// A node joins the scope of its cell, and a process that runs a controller joins the
// root scope. [Membership.Join] returns a [Session], whose lease the backend renews
// until the context of the join ends or the node leaves. [Membership.Watch] yields
// each [View] of a scope in order, so the controller of the scope follows its live
// nodes.
//
// # Leases
//
// A session lasts as long as its lease. The backend expires the lease at its TTL after
// the latest renewal that it applied. The node closes Done at the validity of the lease
// after the send time of the latest renewal that the backend accepted. The validity is
// shorter than the TTL, so the node stops before the backend gives its replicas to
// another node.
//
// # Incarnations
//
// A membership is bound to a namespace incarnation, like the store. Join and Watch
// return shard.ErrIncarnation for a scope that another incarnation created, so a
// delayed call of a deleted namespace does not change anything in its successor.
//
// # Concurrency
//
// Every implementation of [Membership] and [Session] is safe for concurrent use. A copy
// of a [View] shares the elements of its Nodes with the original.
//
// # Dependency position
//
// membership imports the standard library, the package epoch of go.thesmos.sh/core,
// and the packages ownership and shard of go.thesmos.sh/sharder.
package membership
