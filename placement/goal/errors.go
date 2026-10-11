// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package goal

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// ErrConfig reports a configuration of a goal that is not valid, such as a goal of
// balance without a metric. It classifies as errs.Invalid.
var ErrConfig = errs.WithClass(errors.New("goal: invalid configuration"), errs.Invalid)
