// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package proposal

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// ErrUnknownOperationKind reports an OperationKind that is not one of the five kinds. A
// codec returns an error that wraps it for such a kind other than zero, because an
// encoding leaves out a field of the zero kind. ParseOperationKind and
// OperationKind.UnmarshalText return one for text that is not the spelling of a kind. It
// classifies as errs.Invalid.
var ErrUnknownOperationKind = errs.WithClass(errors.New("proposal: unknown operation kind"), errs.Invalid)
