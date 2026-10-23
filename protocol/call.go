// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package protocol

//go:generate go tool kanon -type=Call -validate=valid

import "fmt"

// Call is a replica callback that a controller sends to the agent of a node, such as
// [Add]. The callbacks of the replica state model primary-only are the two of this
// release.
//
// [Call.IsValid] refuses the zero Call. An encoding leaves out a field of the zero Call,
// so a codec does not check it, and the agent that receives a request checks
// [Call.IsValid].
//
// # Encoding
//
// kanon encodes a Call as an unsigned varint of its number. A codec returns an error that
// wraps [ErrUnknownCall] for a Call other than zero outside the two callbacks, on encode
// and on decode. [Call.String] returns the spelling of a Call, such as "add", and the form
// "Call(n)" for any other value. [ParseCall] returns the Call of a spelling.
//
// # Concurrency
//
// A Call is a comparable value.
//
// # Allocation contract
//
// [Call.IsValid], and [Call.String] of one of the two callbacks, do not allocate. String
// allocates the form of any other value.
//
//dokimi:enum text=comment
type Call uint8

const (
	// Add starts a replica on the node in the role of the request.
	Add Call = 1 // add
	// Drop stops the replica on the node and removes its state.
	Drop Call = 2 // drop
)

// valid returns nil for one of the two callbacks. For any other callback, it returns an
// error that wraps [ErrUnknownCall] and contains the number of c. The generated
// ValidateKanon calls it, so a codec refuses a callback outside the two.
func (c Call) valid() error {
	if !c.IsValid() {
		return fmt.Errorf("%w: %d", ErrUnknownCall, uint8(c))
	}
	return nil
}
