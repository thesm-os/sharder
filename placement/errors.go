// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placement

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// ErrInput reports an input that NewState cannot index, such as a layout with two
// shards of one ID, or a replica in a role that the model does not declare. It
// classifies as errs.Invalid.
var ErrInput = errs.WithClass(errors.New("placement: invalid input"), errs.Invalid)

// ErrMove reports a move that State.Apply cannot apply, such as a move that adds a
// replica to a node that already hosts a replica of the shard. It classifies as
// errs.Invalid.
var ErrMove = errs.WithClass(errors.New("placement: move does not apply to the state"), errs.Invalid)

// ErrPlan reports a plan that fails a check of Validate, such as a target that adds a
// replica that a constraint rejects. It classifies as errs.Invalid.
var ErrPlan = errs.WithClass(errors.New("placement: invalid plan"), errs.Invalid)

// ErrCost reports a cost of a goal that is not a finite number. It classifies as
// errs.Invalid.
var ErrCost = errs.WithClass(errors.New("placement: cost is not finite"), errs.Invalid)

// ErrNoNode reports a replica that no node can host. The Reason of a Target with a
// deficit wraps it. It classifies as errs.Conflict, because a later state can admit
// the replica.
var ErrNoNode = errs.WithClass(errors.New("placement: no feasible node"), errs.Conflict)
