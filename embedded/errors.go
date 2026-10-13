// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package embedded

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// The errors of a process.
var (
	// ErrConfig is the error of New for a Config that it refuses. It classifies as
	// errs.Invalid.
	ErrConfig = errs.WithClass(errors.New("embedded: invalid configuration"), errs.Invalid)
	// ErrStarted is the error of a second Start, and of a Start after Close. A process runs
	// once. It classifies as errs.Invalid.
	ErrStarted = errs.WithClass(errors.New("embedded: the process has started"), errs.Invalid)
)
