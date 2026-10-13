// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package routes

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// The sentinel errors of package routes.
var (
	// ErrConfig reports a Config without one of its required fields. It classifies as
	// errs.Invalid.
	ErrConfig = errs.WithClass(errors.New("routes: invalid configuration"), errs.Invalid)

	// ErrClosed reports a call after the router has stopped. Route returns it, and so does
	// each Refresh that waits when the router stops. It classifies as errs.Invalid.
	ErrClosed = errs.WithClass(errors.New("routes: the router has stopped"), errs.Invalid)

	// ErrMapping reports a namespace whose positions the router cannot compute: a spec that
	// does not resolve with the registry of the router, a partitioner whose mapping differs
	// from the mapping that the root recorded, or a placement that starts at no slot of its
	// cell. It classifies as errs.Unsupported.
	ErrMapping = errs.WithClass(errors.New("routes: the router cannot compute the mapping"), errs.Unsupported)

	// ErrNoRoute reports a key whose shard has no route: a shard without a placement, or a
	// shard whose replicas are on nodes that the view of its cell does not list. Route
	// returns it. It classifies as errs.Transient.
	ErrNoRoute = errs.WithClass(errors.New("routes: no route"), errs.Transient)

	// ErrCell reports a Refresh of a cell that the cell map does not have. It classifies as
	// errs.NotFound.
	ErrCell = errs.WithClass(errors.New("routes: no such cell"), errs.NotFound)
)
