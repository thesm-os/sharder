// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package budget

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// The sentinel errors of package budget.
var (
	// ErrUnknownImpact reports an Impact that is not one of the three impacts. A codec
	// returns an error that wraps it for such an impact other than zero, because an
	// encoding leaves out a field of the zero impact. It classifies as errs.Invalid.
	ErrUnknownImpact = errs.WithClass(errors.New("budget: unknown impact"), errs.Invalid)

	// ErrReusedID reports a disruption whose ID the scope has recorded for another
	// disruption. A Budget returns an error that wraps it, whether the reservation of
	// the ID is pending or admitted. It classifies as errs.Invalid.
	ErrReusedID = errs.WithClass(errors.New("budget: disruption ID reused for another disruption"), errs.Invalid)
)
