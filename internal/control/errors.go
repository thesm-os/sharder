// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package control

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// The sentinel errors of package control.
var (
	// ErrConfig reports a Config without one of its required fields. New returns an error
	// that wraps it. It classifies as errs.Invalid.
	ErrConfig = errs.WithClass(errors.New("control: invalid configuration"), errs.Invalid)

	// ErrSpec reports a spec that the controller cannot apply, such as a spec whose
	// strategies do not build, or a scope whose shards differ from the spans of its
	// partitioner. It also reports a spec change that the namespace cannot take. It
	// classifies as errs.Invalid.
	ErrSpec = errs.WithClass(errors.New("control: the spec does not apply"), errs.Invalid)

	// ErrNotServing reports a call that the controller receives before it serves, or after
	// it has stopped. The caller sends the call again, to the controller or to its next
	// controller. It classifies as errs.Transient.
	ErrNotServing = errs.WithClass(errors.New("control: the controller does not serve"), errs.Transient)

	// ErrDenied reports a call whose caller the controller does not authorise for it. It
	// classifies as errs.Denied.
	ErrDenied = errs.WithClass(errors.New("control: the caller is not authorised"), errs.Denied)

	// ErrCall reports a call that the controller refuses before any effect, such as a call
	// for another scope, a proposal whose Expires has passed, or a disruption without
	// nodes. It classifies as errs.Invalid.
	ErrCall = errs.WithClass(errors.New("control: invalid call"), errs.Invalid)

	// ErrStale reports a call for a state that the controller no longer has: a node
	// incarnation that is not live, or the Base of a proposal that the state has moved
	// past. It classifies as errs.Conflict.
	ErrStale = errs.WithClass(errors.New("control: the call names a state that has changed"), errs.Conflict)

	// ErrApplied reports a proposal whose ID the scope has applied, or applies with a commit
	// in flight. The caller reads the outcome from the Applied of the scope. It classifies
	// as errs.Conflict.
	ErrApplied = errs.WithClass(errors.New("control: the scope has applied the proposal"), errs.Conflict)

	// ErrBusy reports a move proposal that the bounds on the transitions in flight refuse
	// now. The caller submits a new proposal later. It classifies as errs.Transient.
	ErrBusy = errs.WithClass(errors.New("control: the bounds of the controller refuse the move now"),
		errs.Transient)
)
