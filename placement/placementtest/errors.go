// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placementtest

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// ErrUnconstrained reports a subject of PlaceReturnsValidPlans that does not implement
// Constrained. The check ends with a fault that wraps it, and has no verdict on the
// subject. It classifies as errs.Invalid.
var ErrUnconstrained = errs.WithClass(errors.New("placementtest: the placer does not implement Constrained"),
	errs.Invalid)
