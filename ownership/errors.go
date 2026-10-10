// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package ownership

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// ErrConfig reports a configuration of exclusive ownership that is not valid, such as
// a lease whose renewal interval is not below its validity. It classifies as
// errs.Invalid.
var ErrConfig = errs.WithClass(errors.New("ownership: invalid configuration"), errs.Invalid)
