// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package planner

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// ErrConfig reports a configuration of the planner that is not valid, such as a goal
// without a name, or constraints and goals that do not match their selections. It
// classifies as errs.Invalid.
var ErrConfig = errs.WithClass(errors.New("planner: invalid configuration"), errs.Invalid)
