// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package cell

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// The sentinel errors of package cell.
var (
	// ErrConfig reports a Config without one of its required fields. New returns an error
	// that wraps it. It classifies as errs.Invalid.
	ErrConfig = errs.WithClass(errors.New("cell: invalid configuration"), errs.Invalid)

	// ErrRoot reports a root scope that does not place the cell, and a read of the root
	// scope that fails. The cell stops with an error that wraps it. It classifies as
	// errs.Conflict.
	ErrRoot = errs.WithClass(errors.New("cell: the root scope does not place the cell"), errs.Conflict)
)
