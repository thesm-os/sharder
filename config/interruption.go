// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package config

//go:generate go run golang.org/x/tools/cmd/stringer@v0.52.0 -type=Interruption -linecomment -output=interruption.string_gen.go

import "fmt"

// Interruption is the interruption of service that planned work may cause. Every
// handoff protocol of the support matrix leaves a shard without a serving writer for a
// writer gap, so Resolve accepts only [PlannedInterruption].
//
// Resolve fills a zero Interruption from the preset, and refuses a spec whose preset
// does not have one.
//
// # Encoding
//
// [Interruption.String] returns the spelling of an interruption, such as "planned".
// The text methods write and read the spelling, so encoding/json writes an
// Interruption as a string.
//
// # Concurrency
//
// An Interruption is a comparable value.
type Interruption uint8

// PlannedInterruption is the interruption under which a planned move or a planned
// disruption may leave a shard without a serving writer for the writer gap of the
// handoff protocol. The budget counts the replica of the shard as unavailable for the
// gap.
const PlannedInterruption Interruption = 1 // planned

// Valid reports whether i is PlannedInterruption. The zero Interruption is not.
func (i Interruption) Valid() bool {
	return i == PlannedInterruption
}

// MarshalText returns the spelling of i, such as "planned".
//
// Error modes:
//   - an error that wraps ErrSpec when i is not Valid.
func (i Interruption) MarshalText() ([]byte, error) {
	if !i.Valid() {
		return nil, fmt.Errorf("%w: unknown interruption %d", ErrSpec, uint8(i))
	}
	return []byte(i.String()), nil
}

// UnmarshalText sets i to the interruption whose spelling is text. It leaves i
// unchanged when it returns an error.
//
// Error modes:
//   - an error that wraps ErrSpec when text is not the spelling of an interruption.
func (i *Interruption) UnmarshalText(text []byte) error {
	if string(text) != PlannedInterruption.String() {
		return fmt.Errorf("%w: unknown interruption %q", ErrSpec, text)
	}
	*i = PlannedInterruption
	return nil
}
