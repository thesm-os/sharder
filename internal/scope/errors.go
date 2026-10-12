// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package scope

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// The sentinel errors of package scope.
var (
	// ErrConfig reports a Config without one of its required fields. New returns an error
	// that wraps it. It classifies as errs.Invalid.
	ErrConfig = errs.WithClass(errors.New("scope: invalid configuration"), errs.Invalid)

	// ErrNotAcquired reports a commit before the acquisition of the scope has completed,
	// or after the scope has stopped. Commit returns an error that wraps it. It
	// classifies as errs.Invalid.
	ErrNotAcquired = errs.WithClass(errors.New("scope: the scope is not acquired"), errs.Invalid)

	// ErrSchema reports a scope whose schema is above the schema of this release, which
	// the release cannot read. The acquisition stops with an error that wraps it before
	// it commits anything. It classifies as errs.Unsupported.
	ErrSchema = errs.WithClass(errors.New("scope: the schema of the scope is above the schema of this release"),
		errs.Unsupported)

	// ErrChanged reports a scope whose generation moved without a commit of its writer,
	// such as a scope that an operator deleted. The scope stops with an error that wraps
	// it. It classifies as errs.Conflict.
	ErrChanged = errs.WithClass(errors.New("scope: the scope changed without a commit of its writer"), errs.Conflict)
)
