// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package root

import (
	"errors"
	"fmt"

	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/internal/control"
)

// ErrConfig reports a Config without one of its required fields, or with a spec that does
// not apply. New returns an error that wraps it. It classifies as errs.Invalid.
var ErrConfig = errs.WithClass(errors.New("root: invalid configuration"), errs.Invalid)

// errCellCall is the error of the reply to a report, a disruption and the end of a
// disruption. Only the controller of a cell takes these calls.
var errCellCall = fmt.Errorf("%w: only the controller of a cell takes reports, disruptions and the ends of disruptions",
	control.ErrCall)
