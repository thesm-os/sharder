// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent

import (
	"errors"
	"fmt"

	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"
)

// The sentinel errors of package agent.
var (
	// ErrConfig reports a Config without one of its required fields, a namespace that the
	// node cannot serve, and a Close without a deadline. It classifies as errs.Invalid.
	ErrConfig = errs.WithClass(errors.New("agent: invalid configuration"), errs.Invalid)

	// ErrNotWriter reports a shard that the node does not write. Machine.Token returns it.
	// It classifies as errs.Conflict.
	ErrNotWriter = errs.WithClass(errors.New("agent: the node is not the writer of the shard"), errs.Conflict)

	// ErrNotServing reports a call that the agent does not serve, such as a callback for an
	// incarnation of the node that the agent has not joined yet, and a call that waits when
	// the agent stops. It classifies as errs.Transient.
	ErrNotServing = errs.WithClass(errors.New("agent: not serving"), errs.Transient)

	// ErrUnseen reports a callback of a controller whose record the agent has not read yet.
	// It classifies as errs.Transient, because the watch of the agent delivers the record.
	ErrUnseen = errs.WithClass(errors.New("agent: the record of the controller is not read yet"), errs.Transient)

	// ErrDenied reports a callback of a caller other than the controller of the cell. It
	// classifies as errs.Denied.
	ErrDenied = errs.WithClass(errors.New("agent: the caller is not the controller of the cell"), errs.Denied)

	// ErrStale reports a callback that the state of the node has moved past. It classifies
	// as errs.Conflict.
	ErrStale = errs.WithClass(errors.New("agent: stale callback"), errs.Conflict)

	// ErrReused reports the key of a callback that the agent has seen with another payload.
	// It classifies as errs.Invalid.
	ErrReused = errs.WithClass(errors.New("agent: the key of the callback has another payload"), errs.Invalid)

	// ErrRequest reports a request that is not valid, such as a request that does not
	// encode, a callback on the path protocol.Cleanup, a callback without an epoch, or a
	// report of a fault that is not a fault. It classifies as errs.Invalid.
	ErrRequest = errs.WithClass(errors.New("agent: invalid request"), errs.Invalid)

	// ErrNoReplica reports a shard without a replica on the node. It classifies as
	// errs.NotFound.
	ErrNoReplica = errs.WithClass(errors.New("agent: the node has no replica of the shard"), errs.NotFound)

	// ErrNoFault reports the recovery of a replica without a transient fault. It classifies
	// as errs.Conflict.
	ErrNoFault = errs.WithClass(errors.New("agent: the replica has no transient fault"), errs.Conflict)
)

// errRevoked is the error of Machine.Token for a tenure that has ended or whose lease has
// passed. It wraps epoch.ErrFenced, so it classifies as errs.Conflict.
var errRevoked = fmt.Errorf("agent: the tenure of the shard has ended: %w", epoch.ErrFenced)
