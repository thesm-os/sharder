// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package router

import (
	"errors"
	"fmt"

	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/internal/machine/routes"
	"go.thesmos.sh/sharder/shard"
)

// The sentinel errors of a router. The errors other than ErrConfig are the errors of the
// machine of the router, so errors.Is matches them as Watcher.Route returns them.
var (
	// ErrConfig is the error of New for a Config that New refuses, of Do for fewer than one
	// attempt, and of Watcher.Refresh for a request that lacks a namespace incarnation or a
	// cell. It classifies as errs.Invalid.
	ErrConfig = errs.WithClass(errors.New("router: invalid configuration"), errs.Invalid)
	// ErrClosed is the error of Watcher.Route and Watcher.Refresh once the run of the router
	// has ended. A Refresh that waits when the run ends gets it too. It classifies as
	// errs.Invalid.
	ErrClosed = routes.ErrClosed
	// ErrNoRoute is the error of Watcher.Route for a key whose shard has no route. A shard
	// without a placement has no route, and neither has a shard whose replicas are on nodes
	// that the latest view of its cell does not list. It classifies as errs.Transient.
	ErrNoRoute = routes.ErrNoRoute
	// ErrMapping is the error of New and of Watcher.Route for a namespace whose positions the
	// router cannot compute, such as a spec that the registry does not resolve. It classifies
	// as errs.Unsupported.
	ErrMapping = routes.ErrMapping
	// ErrCell is the error of Watcher.Refresh for a cell that the cell map of the namespace
	// does not have. It classifies as errs.NotFound.
	ErrCell = routes.ErrCell
)

// NotOwnerError is the error of a server that does not serve the shard of a request at the
// epoch and the namespace incarnation of the request. The server returns it before it starts
// the request, and never for a request that it has started. [Do] routes the key of the
// request again after it.
//
// # Concurrency
//
// A NotOwnerError is a value, and its methods do not modify it.
type NotOwnerError struct {
	// Cell is the cell of the shard.
	Cell shard.CellID
	// Hint is the node that the server expects to serve the shard. It is empty when the server
	// does not expect a node.
	Hint shard.NodeID
	// Shard is the shard of the request.
	Shard shard.ID
	// Epoch is the epoch of the request.
	Epoch epoch.Epoch
	// Generation is the generation of the cell in Incarnation that the server has observed.
	Generation epoch.Epoch
	// Incarnation is the namespace incarnation of the server.
	Incarnation shard.NamespaceIncarnation
}

// Error returns a text with every field of e.
func (e *NotOwnerError) Error() string {
	return fmt.Sprintf(
		"router: the server does not serve shard %s of cell %s at epoch %d: incarnation %s, generation %d, hint %q",
		e.Shard, e.Cell, e.Epoch, e.Incarnation, e.Generation, e.Hint,
	)
}

// Class returns errs.Conflict. The shard of the request has moved, so the caller routes the
// key again before it retries.
func (*NotOwnerError) Class() errs.Class {
	return errs.Conflict
}
