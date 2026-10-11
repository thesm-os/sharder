// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// The sentinel errors of package shard.
var (
	// ErrIncarnation reports a commit, a callback or a report for an incarnation of
	// the namespace other than the receiver's. It classifies as errs.Conflict,
	// because the sender acts on a namespace that no longer exists. The sender reads
	// the root scope again.
	ErrIncarnation = errs.WithClass(errors.New("shard: namespace incarnation mismatch"), errs.Conflict)

	// ErrUnknownRole reports a Role that is not one of the eight roles. A codec and
	// Role.MarshalText return an error that wraps it for such a Role.
	// Role.UnmarshalText returns one for text that is not the spelling of a role. It
	// classifies as errs.Invalid.
	ErrUnknownRole = errs.WithClass(errors.New("shard: unknown role"), errs.Invalid)

	// ErrUnknownStatus reports a Status that is not one of the four statuses. A codec
	// returns an error that wraps it for such a Status other than zero, because an
	// encoding leaves out a field of the zero Status. It classifies as errs.Invalid.
	ErrUnknownStatus = errs.WithClass(errors.New("shard: unknown status"), errs.Invalid)

	// ErrUnknownFault reports a Fault that is not one of the three faults. A codec
	// returns an error that wraps it for such a Fault. It classifies as errs.Invalid.
	ErrUnknownFault = errs.WithClass(errors.New("shard: unknown fault"), errs.Invalid)

	// ErrUnknownPhase reports a Phase that is not one of the four phases. A codec
	// returns an error that wraps it for such a Phase other than zero. It classifies as
	// errs.Invalid.
	ErrUnknownPhase = errs.WithClass(errors.New("shard: unknown phase"), errs.Invalid)

	// ErrDuration reports text that is not a Go duration string, such as "30s".
	// Duration.UnmarshalText returns an error that wraps it. It classifies as
	// errs.Invalid.
	ErrDuration = errs.WithClass(errors.New("shard: invalid duration"), errs.Invalid)
)
