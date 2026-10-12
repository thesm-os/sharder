// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package follow

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// ErrConfig reports a Config without one of its required fields. New returns an error that
// wraps it. It classifies as errs.Invalid.
var ErrConfig = errs.WithClass(errors.New("follow: invalid configuration"), errs.Invalid)
