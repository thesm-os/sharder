// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package storetest

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// ErrUnknownLoss reports text that is not the spelling of a loss. ParseLoss and
// Loss.UnmarshalText return an error that wraps it. It classifies as errs.Invalid.
var ErrUnknownLoss = errs.WithClass(errors.New("storetest: unknown loss"), errs.Invalid)
