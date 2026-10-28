// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package membershiptest

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// The sentinel errors of package membershiptest.
var (
	// ErrUnknownLoss reports text that is not the spelling of a loss. ParseLoss and
	// Loss.UnmarshalText return an error that wraps it. It classifies as errs.Invalid.
	ErrUnknownLoss = errs.WithClass(errors.New("membershiptest: unknown loss"), errs.Invalid)

	// ErrOperation reports a check that calls an operation of the kit, such as Backed, on
	// a membership that does not implement it. The check ends with a fault that wraps it.
	// It classifies as errs.Unsupported.
	ErrOperation = errs.WithClass(errors.New("membershiptest: the membership does not implement the operation"),
		errs.Unsupported)
)
