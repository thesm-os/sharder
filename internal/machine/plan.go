// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machine

import (
	"go.thesmos.sh/sharder/placement"
)

// Plan is a planning pass over one cell. The runtime runs placement.Placer.Place over
// Input and then placement.Validate over the plan with Constraints. Both run on goroutines
// of the runtime, and the runtime passes the result as a [Planned].
type Plan struct {
	// Placer is the placer of the namespace.
	Placer placement.Placer
	// Input is the input of the pass. The pass reads it while the machine takes further
	// events, so the machine passes a copy of its assignment in Input.Current.
	Input *placement.Input
	// Constraints are the constraints that placement.Validate checks.
	Constraints []placement.Constraint
	// Token is the token of the effect.
	Token Token
}

var _ Effect = Plan{}

// effect marks Plan as an Effect.
func (Plan) effect() {}

// Planned is the result of a [Plan].
type Planned struct {
	// Err is the error of placement.Placer.Place, the error of placement.Validate for a
	// plan that it refused, or nil.
	Err error
	// Violations lists the violations that placement.Validate returned.
	Violations []placement.Violation
	// Plan is the plan that placement.Placer.Place returned, also when placement.Validate
	// refused it.
	Plan placement.Plan
	// Token is the token of the Plan.
	Token Token
}

var _ Event = Planned{}

// event marks Planned as an Event.
func (Planned) event() {}
