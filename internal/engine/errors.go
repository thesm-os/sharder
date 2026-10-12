// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package engine

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// The sentinel errors of package engine.
var (
	// ErrConfig reports a Config without one of its required fields, or with a bound below
	// one. New returns an error that wraps it. It classifies as errs.Invalid.
	ErrConfig = errs.WithClass(errors.New("engine: invalid configuration"), errs.Invalid)

	// ErrMove reports a move that the Engine cannot start. Start returns an error that wraps
	// it for a move of a shard that has a transition in flight, and for a move that the
	// protocol cannot script. It classifies as errs.Invalid.
	ErrMove = errs.WithClass(errors.New("engine: invalid move"), errs.Invalid)

	// ErrRecord reports a record of a transition that the Engine cannot continue, such as a
	// record with a negative step. Resume returns an error that wraps it. It classifies as
	// errs.Invalid.
	ErrRecord = errs.WithClass(errors.New("engine: invalid record of a transition"), errs.Invalid)

	// ErrProtocol reports a record of a transition that the release of the Engine does not
	// implement. Such a record has another protocol, another version of the protocol, or a
	// phase that the lifecycle of a transition does not have. Resume returns an error that
	// wraps it. It classifies as errs.Unsupported.
	ErrProtocol = errs.WithClass(errors.New("engine: unsupported record of a transition"), errs.Unsupported)
)
