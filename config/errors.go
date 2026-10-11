// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package config

import (
	"errors"
	"fmt"

	"go.thesmos.sh/core/errs"
)

// The sentinel errors of package config.
var (
	// ErrDuplicate reports a second factory of one name on one axis, or a second
	// preset of one name. Register and RegisterPreset return an error that wraps it.
	// It classifies as errs.Invalid.
	ErrDuplicate = errs.WithClass(errors.New("config: duplicate registration"), errs.Invalid)

	// ErrUnknown reports a strategy name without a factory on its axis, or a preset
	// name without a preset. Resolve, Build and BuildNamed return an error that wraps
	// it. It classifies as errs.Invalid.
	ErrUnknown = errs.WithClass(errors.New("config: unknown name"), errs.Invalid)

	// ErrUnsupported reports a selection of a spec or of a preset outside the support
	// matrix of a registry. It also reports a capability that the matrix requires and
	// a namespace does not have. It classifies as errs.Unsupported, and errors.Is
	// matches it with errors.ErrUnsupported.
	ErrUnsupported = errs.WithClass(
		fmt.Errorf("config: outside the support matrix: %w", errors.ErrUnsupported), errs.Unsupported)

	// ErrSpec reports a spec or a strategy configuration that breaks a rule, such as a
	// value out of range or a requirement that the namespace does not meet. Resolve,
	// Build, BuildNamed and the text methods of Interruption return an error that
	// wraps it. When a factory fails, that error also wraps the error of the factory.
	// It classifies as errs.Invalid.
	ErrSpec = errs.WithClass(errors.New("config: invalid spec"), errs.Invalid)

	// ErrRegistration reports a registration or a build that the registry cannot
	// perform, such as a factory with an empty name or a factory of another type.
	// Register, RegisterPreset, Build, BuildNamed and Resolve return an error that
	// wraps it. It classifies as errs.Invalid.
	ErrRegistration = errs.WithClass(errors.New("config: invalid registration"), errs.Invalid)

	// ErrChange reports a change of a running namespace that a spec edit cannot make,
	// such as a change of the mapping. Change returns an error that wraps it. The
	// text of that error contains the operation that the change needs. It classifies
	// as errs.Invalid.
	ErrChange = errs.WithClass(errors.New("config: change needs more than a spec edit"), errs.Invalid)
)
