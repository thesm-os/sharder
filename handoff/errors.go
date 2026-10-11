// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package handoff

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// ErrScript reports a script that Script.Validate refuses for its transfer, such as a
// step on a side that the transfer does not have. It classifies as errs.Invalid.
var ErrScript = errs.WithClass(errors.New("handoff: invalid script"), errs.Invalid)

// ErrTransfer reports a transfer that a protocol cannot script, such as a transfer
// without a To for a protocol that adds a replica. It classifies as errs.Invalid.
var ErrTransfer = errs.WithClass(errors.New("handoff: the protocol cannot script the transfer"), errs.Invalid)
