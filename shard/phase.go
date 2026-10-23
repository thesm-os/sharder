// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard

//go:generate go tool kanon -type=Phase -validate=valid

import "fmt"

// Phase is the phase of a transition in its lifecycle, such as [PhaseAborting]. The
// store records the phase of every transition in flight, and the lifecycle of package
// handoff moves a transition from one phase to the next.
//
// [Phase.IsValid] refuses the zero Phase. An encoding leaves out a field of the zero
// Phase, so a codec does not check it, and the reader of a transition checks
// [Phase.IsValid].
//
// # Encoding
//
// kanon encodes a Phase as an unsigned varint of its number. A codec returns an error
// that wraps [ErrUnknownPhase] for a Phase other than zero outside the four phases, on
// encode and on decode. [Phase.String] returns the spelling of a Phase, such as
// "aborting", and the form "Phase(n)" for any other value. [ParsePhase] returns the Phase
// of a spelling.
//
// # Concurrency
//
// A Phase is a comparable value.
//
// # Allocation contract
//
// [Phase.IsValid], and [Phase.String] of one of the four phases, do not allocate. String
// allocates the form of any other value.
//
//dokimi:enum text=comment
type Phase uint8

const (
	// PhaseRunning is the phase of a transition that runs the steps of its script.
	PhaseRunning Phase = 1 // running
	// PhaseAborting is the phase of a transition that runs the abort path of the step
	// that failed.
	PhaseAborting Phase = 2 // aborting
	// PhaseDone is the phase of a transition whose every step completed. The commit
	// that ends a transition removes its record, so the store does not keep this phase.
	PhaseDone Phase = 3 // done
	// PhaseAborted is the phase of a transition whose abort path completed or was
	// abandoned. The store does not keep this phase either.
	PhaseAborted Phase = 4 // aborted
)

// valid returns nil for one of the four phases. For any other phase, it returns an error
// that wraps [ErrUnknownPhase] and contains the number of p. The generated ValidateKanon
// calls it, so a codec refuses a phase outside the four.
func (p Phase) valid() error {
	if !p.IsValid() {
		return fmt.Errorf("%w: %d", ErrUnknownPhase, uint8(p))
	}
	return nil
}
