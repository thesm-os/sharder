// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package fixedslots

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// ErrConfig reports a configuration of fixed-slots that is not valid, such as one
// without slots or without a hash function. It classifies as errs.Invalid.
var ErrConfig = errs.WithClass(errors.New("fixedslots: invalid configuration"), errs.Invalid)
