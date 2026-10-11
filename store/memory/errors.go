// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package memory

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// The sentinel errors of package memory.
var (
	// ErrOutage reports a call during an outage that StartOutage began. A Commit
	// returns an error that wraps it and version.ErrOutcomeUnknown, because a commit to a
	// real backend can apply during an outage without a reply. It classifies as
	// errs.Transient.
	ErrOutage = errs.WithClass(errors.New("memory: the store is unavailable"), errs.Transient)

	// ErrFault reports a commit or a renewal that a fault point of FailAt interrupted.
	// It classifies as errs.Transient.
	ErrFault = errs.WithClass(errors.New("memory: injected fault"), errs.Transient)

	// ErrClosed reports a call after Close. It classifies as errs.Invalid.
	ErrClosed = errs.WithClass(errors.New("memory: the store is closed"), errs.Invalid)

	// ErrToken reports a page token that Read did not write. It classifies as
	// errs.Invalid.
	ErrToken = errs.WithClass(errors.New("memory: malformed page token"), errs.Invalid)
)

// errNodeID reports a join of a node without an ID. It classifies as errs.Invalid, as
// membership.Membership.Join states.
var errNodeID = errs.WithClass(errors.New("memory: node without an ID"), errs.Invalid)
