// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package handoff

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// The sentinel errors of package handoff.
var (
	// ErrScript reports a script that Script.Validate refuses for its transfer, such as
	// a step on a side that the transfer does not have. It classifies as errs.Invalid.
	ErrScript = errs.WithClass(errors.New("handoff: invalid script"), errs.Invalid)

	// ErrTransfer reports a transfer that a protocol cannot script, such as a transfer
	// without a To for a protocol that adds a replica. It classifies as errs.Invalid.
	ErrTransfer = errs.WithClass(errors.New("handoff: the protocol cannot script the transfer"), errs.Invalid)

	// ErrUnknownSide reports text that is not the spelling of a side. ParseSide and
	// Side.UnmarshalText return an error that wraps it. It classifies as errs.Invalid.
	ErrUnknownSide = errs.WithClass(errors.New("handoff: unknown side"), errs.Invalid)

	// ErrUnknownEvent reports text that is not the spelling of an event. ParseEvent and
	// Event.UnmarshalText return an error that wraps it. It classifies as errs.Invalid.
	ErrUnknownEvent = errs.WithClass(errors.New("handoff: unknown event"), errs.Invalid)

	// ErrUnknownAction reports text that is not the spelling of an action. ParseAction
	// and Action.UnmarshalText return an error that wraps it. It classifies as
	// errs.Invalid.
	ErrUnknownAction = errs.WithClass(errors.New("handoff: unknown action"), errs.Invalid)
)
