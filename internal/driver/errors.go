// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package driver

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// The sentinel errors of package driver.
var (
	// ErrConfig reports a Config without one of its required fields, and an effect whose
	// dependency the Config does not have. It classifies as errs.Invalid.
	ErrConfig = errs.WithClass(errors.New("driver: invalid configuration"), errs.Invalid)

	// ErrRun reports a second call of Run. A Driver runs its machine once. It classifies as
	// errs.Invalid.
	ErrRun = errs.WithClass(errors.New("driver: a driver runs once"), errs.Invalid)

	// ErrStopped reports a call after the run has stopped, and a call that the run did not
	// reply to before it returned. It classifies as errs.Invalid.
	ErrStopped = errs.WithClass(errors.New("driver: the run has stopped"), errs.Invalid)

	// ErrReply reports a reply of the machine whose type is not the type that its call
	// expects. It classifies as errs.Invalid.
	ErrReply = errs.WithClass(errors.New("driver: the reply has another type"), errs.Invalid)

	// ErrEffect reports an effect that the driver does not perform. Run stops the machine
	// with it. It classifies as errs.Invalid.
	ErrEffect = errs.WithClass(errors.New("driver: unknown effect"), errs.Invalid)

	// ErrEnded reports a watch whose iteration ended while its context had not ended. The
	// driver passes it in the last item of the watch, so the machine watches again. It
	// classifies as errs.Transient.
	ErrEnded = errs.WithClass(errors.New("driver: the watch ended"), errs.Transient)
)
