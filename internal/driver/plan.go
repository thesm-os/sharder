// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package driver

import (
	"context"
	"fmt"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/placement"
)

// plan runs the Place of the placer of e over the input of e, and then placement.Validate
// over the plan with the constraints of e. It returns the Planned of e, with the plan of
// Place also when Place fails or Validate refuses the plan.
func (*Driver) plan(ctx context.Context, e *machine.Plan) machine.Event {
	if e.Placer == nil {
		return machine.Planned{Err: fmt.Errorf("%w: a Plan without a placer", ErrConfig), Token: e.Token}
	}
	p, err := e.Placer.Place(ctx, e.Input)
	if err != nil {
		return machine.Planned{Plan: p, Err: err, Token: e.Token}
	}
	violations, err := placement.Validate(e.Input, p, e.Constraints)
	return machine.Planned{Plan: p, Violations: violations, Err: err, Token: e.Token}
}
