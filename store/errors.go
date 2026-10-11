// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package store

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// The sentinel errors of package store.
var (
	// ErrCompacted reports a read page or a watch at a generation that the store no
	// longer keeps, or a watcher whose buffer overflowed. The reader keeps its last
	// complete state and starts a new snapshot. It classifies as errs.Conflict.
	ErrCompacted = errs.WithClass(errors.New("store: generation compacted"), errs.Conflict)

	// ErrReused reports a commit whose ID the scope has recorded with another digest,
	// because the caller gave one ID to two mutations. It classifies as errs.Invalid.
	ErrReused = errs.WithClass(errors.New("store: commit ID reused with another mutation"), errs.Invalid)

	// ErrTooLarge reports a mutation that no commit to the scope can apply. Fits and
	// Commit return it before anything is sent. It classifies as errs.Invalid.
	ErrTooLarge = errs.WithClass(errors.New("store: mutation exceeds the limits of the scope"), errs.Invalid)
)
