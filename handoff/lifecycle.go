// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package handoff

import (
	"go.thesmos.sh/core/fsm"

	"go.thesmos.sh/sharder/shard"
)

// Event is the outcome of the current step of a transition, such as [Fail]. On each
// event, the engine looks up the next phase of the transition in [Lifecycle].
//
// [Event.IsValid] refuses the zero Event.
//
// # Encoding
//
// [Event.String] returns the spelling of an Event, such as "abandon", and the form
// "Event(n)" for any other value. [ParseEvent] returns the event of a spelling.
//
// # Concurrency
//
// An Event is a comparable value.
//
// # Allocation contract
//
// [Event.IsValid], and [Event.String] of one of the four events, do not allocate. String
// allocates the form of any other value.
//
//dokimi:enum text=comment
type Event uint8

const (
	// Advance reports that a step other than the last one of the current path
	// completed.
	Advance Event = 1 // advance
	// Finish reports that the last step of the current path completed, or that the path
	// has no steps.
	Finish Event = 2 // finish
	// Fail reports that the current step failed. A step of the forward path fails after
	// its retries, or at once when it fails definitively. A step of the abort path fails
	// after its retries, and the engine then runs the step again.
	Fail Event = 3 // fail
	// Abandon reports that a step of the abort path failed definitively. The incarnation
	// of the node that the step addresses has ended, or the handler of the node returned
	// an error that is not transient.
	Abandon Event = 4 // abandon
)

// Lifecycle is the table of the phases of a transition. A transition starts in
// shard.PhaseRunning, and these edges lead from one phase to the next:
//
//   - Advance leads from shard.PhaseRunning and from shard.PhaseAborting back to the
//     same phase.
//   - Finish leads from shard.PhaseRunning to shard.PhaseDone, and from
//     shard.PhaseAborting to shard.PhaseAborted.
//   - Fail leads from shard.PhaseRunning to shard.PhaseAborting, and from
//     shard.PhaseAborting back to shard.PhaseAborting.
//   - Abandon leads from shard.PhaseAborting to shard.PhaseAborted.
//
// shard.PhaseDone and shard.PhaseAborted are terminal. A transition whose abort path the
// engine abandons ends without an assigned writer. The engine checks Lifecycle.Allows
// before it commits a change of phase to the store. The caller does not assign another
// value to Lifecycle.
//
// # Concurrency
//
// Lifecycle is immutable and safe for concurrent use.
var Lifecycle = fsm.Must(fsm.NewBuilder[shard.Phase, Event, struct{}](shard.PhaseRunning).
	Edge(shard.PhaseRunning, Advance, shard.PhaseRunning).
	Edge(shard.PhaseRunning, Finish, shard.PhaseDone).
	Edge(shard.PhaseRunning, Fail, shard.PhaseAborting).
	Edge(shard.PhaseAborting, Advance, shard.PhaseAborting).
	Edge(shard.PhaseAborting, Finish, shard.PhaseAborted).
	Edge(shard.PhaseAborting, Fail, shard.PhaseAborting).
	Edge(shard.PhaseAborting, Abandon, shard.PhaseAborted).
	Terminal(shard.PhaseDone, shard.PhaseAborted).
	Build())
