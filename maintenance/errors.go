// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package maintenance

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// The sentinel errors of a client.
var (
	// ErrConfig is the error of New for a Config that New refuses. It classifies as
	// errs.Invalid.
	ErrConfig = errs.WithClass(errors.New("maintenance: invalid configuration"), errs.Invalid)
	// ErrNoNamespace is the error of Status, Apply, Drain, Undrain, Move and Explain for a
	// namespace whose root scope has no namespace incarnation, because the root has not created
	// the namespace. It classifies as errs.NotFound.
	ErrNoNamespace = errs.WithClass(errors.New("maintenance: the root has not created the namespace"), errs.NotFound)
	// ErrNoController is the error of every call but Status for a scope whose record does not
	// contain the address of a controller. It classifies as errs.NotFound.
	ErrNoController = errs.WithClass(errors.New("maintenance: the scope has no controller"), errs.NotFound)
	// ErrNoNode is the error of Drain and Undrain for a node that no view of the namespace
	// lists. It classifies as errs.NotFound.
	ErrNoNode = errs.WithClass(errors.New("maintenance: no view of the namespace lists the node"), errs.NotFound)
	// ErrNoShard is the error of Move and Explain for a shard that the placements of no cell of
	// the namespace contain. It classifies as errs.NotFound.
	ErrNoShard = errs.WithClass(errors.New("maintenance: no cell of the namespace has the shard"), errs.NotFound)
)
