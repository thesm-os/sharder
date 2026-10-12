// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// ErrConfig reports a configuration of a built-in strategy that its factory cannot parse
// or build, such as JSON with an unknown field. It classifies as errs.Invalid.
var ErrConfig = errs.WithClass(errors.New("builtin: invalid configuration"), errs.Invalid)
