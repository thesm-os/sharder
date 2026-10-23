// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package budget

//go:generate go tool kanon -type=Impact -validate=valid

import "fmt"

// Impact is what a disruption does to the replicas on its nodes, such as
// [ProcessRestart].
//
// [Impact.IsValid] refuses the zero Impact. An encoding leaves out a field of the zero
// impact, so a codec does not check it, and the controller that receives a disruption
// checks [Impact.IsValid].
//
// # Encoding
//
// kanon encodes an Impact as an unsigned varint of its number. A codec returns an error
// that wraps [ErrUnknownImpact] for an impact other than zero outside the three impacts,
// on encode and on decode. [Impact.String] returns the spelling of an impact, such as
// "process-restart", and the form "Impact(n)" for any other value. [ParseImpact] returns
// the impact of a spelling.
//
// # Concurrency
//
// An Impact is a comparable value.
//
//dokimi:enum text=comment
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

// valid returns nil for one of the three impacts, and an error that wraps
// [ErrUnknownImpact] with the number of i for any other. The generated ValidateKanon
// calls it, so a codec refuses an impact outside the three.
func (i Impact) valid() error {
	if !i.IsValid() {
		return fmt.Errorf("%w: %d", ErrUnknownImpact, uint8(i))
	}
	return nil
}
