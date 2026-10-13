// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package controller

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// The sentinel errors of package controller.
var (
	// ErrConfig reports a Config that New refuses, and a spec of Run that the process
	// cannot run. It classifies as errs.Invalid.
	ErrConfig = errs.WithClass(errors.New("controller: invalid configuration"), errs.Invalid)

	// ErrRun reports a second call of Run. A Controller runs once. It classifies as
	// errs.Invalid.
	ErrRun = errs.WithClass(errors.New("controller: a controller runs once"), errs.Invalid)

	// ErrScope reports a callback for the scope of a cell. A controller process takes the
	// callbacks of a root, and the agent of a node takes the callbacks of its cell. It
	// classifies as errs.Invalid.
	ErrScope = errs.WithClass(errors.New("controller: the callback is not for a root scope"), errs.Invalid)

	// ErrNotServing reports a call for a namespace that the process does not run, and a call
	// for a cell that it does not host. Do, Report, Submit, Disrupt and Done return it
	// before Run and once Run has returned. It classifies as errs.Transient.
	ErrNotServing = errs.WithClass(errors.New("controller: not serving"), errs.Transient)

	// errClosed is the cause with which Close ends the context of the run. Run returns nil for
	// a run that it ends. It classifies as errs.Transient.
	errClosed = errs.WithClass(errors.New("controller: the process has closed"), errs.Transient)
)
