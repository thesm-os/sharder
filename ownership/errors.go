// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package ownership

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// The sentinel errors of package ownership.
var (
	// ErrConfig reports a configuration of exclusive ownership that is not valid, such
	// as a lease whose renewal interval is not below its validity. It classifies as
	// errs.Invalid.
	ErrConfig = errs.WithClass(errors.New("ownership: invalid configuration"), errs.Invalid)

	// ErrUnknownOnLeaseLoss reports text that is not the spelling of an action on the
	// loss of a lease. ParseOnLeaseLoss and OnLeaseLoss.UnmarshalText return an error
	// that wraps it. It classifies as errs.Invalid.
	ErrUnknownOnLeaseLoss = errs.WithClass(errors.New("ownership: unknown action on lease loss"), errs.Invalid)
)
