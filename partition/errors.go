// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package partition

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// ErrRange reports a range that a partitioner cannot divide into shards, such as a
// range of fixed-slots whose bounds are not bounds of slots. It classifies as
// errs.Invalid.
var ErrRange = errs.WithClass(errors.New("partition: range cannot be divided into shards"), errs.Invalid)
