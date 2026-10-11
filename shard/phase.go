// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard

//go:generate go tool kanon -type=Phase -validate=valid
//go:generate go run golang.org/x/tools/cmd/stringer@v0.52.0 -type=Phase -linecomment -output=phase.string_gen.go

import "fmt"

// Phase is the phase of a transition in its lifecycle, such as [PhaseAborting]. The
// store records the phase of every transition in flight, and the lifecycle of package
// handoff moves a transition from one phase to the next.
//
// The zero Phase is not Valid. An encoding leaves out a field of the zero Phase, so a
// codec does not check it, and the reader of a transition checks [Phase.Valid].
//
// # Encoding
//
// kanon encodes a Phase as an unsigned varint of its number. A codec returns an error
// that wraps [ErrUnknownPhase] for a Phase other than zero that is not Valid, on encode
// and on decode. [Phase.String] returns the spelling of a Phase, such as "aborting", and
// the form "Phase(n)" for a value that is not Valid.
//
// # Concurrency
//
// A Phase is a comparable value.
//
// # Allocation contract
//
// [Phase.Valid], and [Phase.String] of a Valid Phase, do not allocate. String allocates
// the form of a value that is not Valid.
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

// Valid reports whether p is one of the four phases. The zero Phase is not.
//
// # Allocation contract
//
// Zero alloc.
func (p Phase) Valid() bool {
	return p >= PhaseRunning && p <= PhaseAborted
}

// valid returns nil for a Valid phase. For any other phase, it returns an error that
// wraps [ErrUnknownPhase] and contains the number of p. The generated ValidateKanon
// calls it, so a codec refuses a phase outside the four.
func (p Phase) valid() error {
	if !p.Valid() {
		return fmt.Errorf("%w: %d", ErrUnknownPhase, uint8(p))
	}
	return nil
}
