// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package budget

//go:generate go tool kanon -type=Impact -validate=valid
//go:generate go run golang.org/x/tools/cmd/stringer@v0.52.0 -type=Impact -linecomment -output=impact.string_gen.go

import "fmt"

// Impact is what a disruption does to the replicas on its nodes, such as
// [ProcessRestart].
//
// The zero Impact is not Valid. An encoding leaves out a field of the zero impact, so
// a codec does not check it, and the controller that receives a disruption checks
// [Impact.Valid].
//
// # Encoding
//
// kanon encodes an Impact as an unsigned varint of its number. A codec returns an
// error that wraps [ErrUnknownImpact] for an impact other than zero that is not Valid,
// on encode and on decode. [Impact.String] returns the spelling of an impact, such as
// "process-restart", and the form "Impact(n)" for a value that is not Valid.
//
// # Concurrency
//
// An Impact is a comparable value.
type Impact uint8

const (
	// NetworkLoss is an impact under which the replicas are unreachable and keep their
	// memory and their disk.
	NetworkLoss Impact = 1 // network-loss
	// ProcessRestart is an impact under which the replicas lose their memory and keep
	// their disk.
	ProcessRestart Impact = 2 // process-restart
	// FullLoss is an impact under which the replicas lose their memory and their disk.
	FullLoss Impact = 3 // full-loss
)

// Valid reports whether i is one of the three impacts. The zero Impact is not.
func (i Impact) Valid() bool {
	return i >= NetworkLoss && i <= FullLoss
}

// valid returns nil for a Valid impact, and an error that wraps [ErrUnknownImpact] with
// the number of i for any other. The generated ValidateKanon calls it, so a codec
// refuses an impact outside the three.
func (i Impact) valid() error {
	if !i.Valid() {
		return fmt.Errorf("%w: %d", ErrUnknownImpact, uint8(i))
	}
	return nil
}
