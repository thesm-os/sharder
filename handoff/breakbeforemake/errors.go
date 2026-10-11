// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package breakbeforemake

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// ErrConfig reports a configuration of break-before-make that is not valid, such as a
// timeout of zero. It classifies as errs.Invalid.
var ErrConfig = errs.WithClass(errors.New("breakbeforemake: invalid configuration"), errs.Invalid)
