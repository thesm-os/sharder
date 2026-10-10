// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package replication

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// ErrInvalidModel reports a model that Compile cannot build a transition table from,
// such as one with two writer roles. It classifies as errs.Invalid.
var ErrInvalidModel = errs.WithClass(errors.New("replication: invalid model"), errs.Invalid)
