// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package proposal

//go:generate go tool kanon -type=OperationKind -validate=valid

import "fmt"

// OperationKind is the kind of change that a proposal requests, such as [Move].
//
// [OperationKind.IsValid] refuses the zero OperationKind. An encoding leaves out a field
// of the zero kind, so a codec does not check it, and the controller that receives a
// proposal checks [OperationKind.IsValid].
//
// # Encoding
//
// kanon encodes an OperationKind as an unsigned varint of its number. A codec returns an
// error that wraps [ErrUnknownOperationKind] for a kind other than zero outside the five
// kinds, on encode and on decode. [OperationKind.String] returns the spelling of a kind,
// such as "move", and the form "OperationKind(n)" for any other value.
// [ParseOperationKind] returns the kind of a spelling.
//
// # Concurrency
//
// An OperationKind is a comparable value.
//
//dokimi:enum text=comment
type OperationKind uint8

const (
	// SpecChange replaces the spec of the namespace. The root applies it, and its base
	// is the version of the spec that it replaces.
	SpecChange OperationKind = 1 // spec
	// Move moves a replica of a shard to another node. The controller of the cell
	// applies it, and its base is the epoch of the shard.
	Move OperationKind = 2 // move
	// Drain moves every replica off a node. The controller of the cell applies it, and
	// its base is the incarnation of the node.
	Drain OperationKind = 3 // drain
	// Undrain lets a drained node take replicas again. The controller of the cell
	// applies it, and its base is the incarnation of the node.
	Undrain OperationKind = 4 // undrain
	// Explain asks for the constraints and the costs of the goals that placed a shard.
	// The controller of the cell returns them and commits nothing, and the base is the
	// generation of the cell.
	Explain OperationKind = 5 // explain
)

// valid returns nil for one of the five kinds, and an error that wraps
// [ErrUnknownOperationKind] and states the number of k for any other. The generated
// ValidateKanon calls it, so a codec refuses a kind outside the five.
func (k OperationKind) valid() error {
	if !k.IsValid() {
		return fmt.Errorf("%w: %d", ErrUnknownOperationKind, uint8(k))
	}
	return nil
}
