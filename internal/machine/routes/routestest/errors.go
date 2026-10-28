// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package routestest

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// ErrUnviewed reports a subject of ViewsAllocateOnlyMovedRoutes that does not implement
// Viewed. The check ends with a fault that wraps it, and has no verdict on the subject. It
// classifies as errs.Invalid.
var ErrUnviewed = errs.WithClass(errors.New("routestest: the router does not implement Viewed"), errs.Invalid)
