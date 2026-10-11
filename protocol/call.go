// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package protocol

//go:generate go tool kanon -type=Call -validate=valid
//go:generate go run golang.org/x/tools/cmd/stringer@v0.52.0 -type=Call -linecomment -output=call.string_gen.go

import "fmt"

// Call is a replica callback that a controller sends to the agent of a node, such as
// [Add]. The callbacks of the replica state model primary-only are the two of this
// release.
//
// The zero Call is not Valid. An encoding leaves out a field of the zero Call, so a codec
// does not check it, and the agent that receives a request checks [Call.Valid].
//
// # Encoding
//
// kanon encodes a Call as an unsigned varint of its number. A codec returns an error that
// wraps [ErrUnknownCall] for a Call other than zero that is not Valid, on encode and on
// decode. [Call.String] returns the spelling of a Call, such as "add", and the form
// "Call(n)" for a value that is not Valid.
//
// # Concurrency
//
// A Call is a comparable value.
//
// # Allocation contract
//
// [Call.Valid], and [Call.String] of a Valid Call, do not allocate. String allocates the
// form of a value that is not Valid.
type Call uint8

const (
	// Add starts a replica on the node in the role of the request.
	Add Call = 1 // add
	// Drop stops the replica on the node and removes its state.
	Drop Call = 2 // drop
)

// Valid reports whether c is Add or Drop. The zero Call is not.
//
// # Allocation contract
//
// Zero alloc.
func (c Call) Valid() bool {
	return c >= Add && c <= Drop
}

// valid returns nil for a Valid callback. For any other callback, it returns an error that
// wraps [ErrUnknownCall] and contains the number of c. The generated ValidateKanon calls
// it, so a codec refuses a callback outside the two.
func (c Call) valid() error {
	if !c.Valid() {
		return fmt.Errorf("%w: %d", ErrUnknownCall, uint8(c))
	}
	return nil
}
