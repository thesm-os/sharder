// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package storetest

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// The sentinel errors of package storetest.
var (
	// ErrUnknownLoss reports text that is not the spelling of a loss. ParseLoss and
	// Loss.UnmarshalText return an error that wraps it. It classifies as errs.Invalid.
	ErrUnknownLoss = errs.WithClass(errors.New("storetest: unknown loss"), errs.Invalid)

	// ErrOperation reports a check that calls an operation of the kit, such as Deleter,
	// on a store that does not implement it. The check ends with a fault that wraps it. It
	// classifies as errs.Unsupported.
	ErrOperation = errs.WithClass(errors.New("storetest: the store does not implement the operation"),
		errs.Unsupported)
)

// errStopped is the error that the planted defect of CampaignsHonourCancellation returns in
// place of the error of the ended context of a campaign.
var errStopped = errors.New("storetest: the campaign stopped")
