// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package protocol

//go:generate go tool kanon -type=Path -validate=valid

import "fmt"

// Path is the path of the script of a transition that a callback belongs to, or the
// cleanup of an agent.
//
// [Path.IsValid] refuses the zero Path. An encoding leaves out a field of the zero Path,
// so a codec does not check it, and the agent that receives a request checks
// [Path.IsValid].
//
// # Encoding
//
// kanon encodes a Path as an unsigned varint of its number. A codec returns an error that
// wraps [ErrUnknownPath] for a Path other than zero outside the three paths, on encode and
// on decode. [Path.String] returns the spelling of a Path, such as "forward", and the form
// "Path(n)" for any other value. [ParsePath] returns the Path of a spelling.
//
// # Concurrency
//
// A Path is a comparable value.
//
// # Allocation contract
//
// [Path.IsValid], and [Path.String] of one of the three paths, do not allocate. String
// allocates the form of any other value.
//
//dokimi:enum text=comment
type Path uint8

const (
	// Forward is the path of the steps of the script, from the first to the last.
	Forward Path = 1 // forward
	// Abort is the path that undoes the move after a forward step fails.
	Abort Path = 2 // abort
	// Cleanup is the path of a drop by an agent of a tenure that the placements that it
	// watches no longer list. Only the agent builds requests on it, and an agent refuses
	// them from a caller.
	Cleanup Path = 3 // cleanup
)

// valid returns nil for one of the three paths. For any other path, it returns an error
// that wraps [ErrUnknownPath] and contains the number of p. The generated ValidateKanon
// calls it, so a codec refuses a path outside the three.
func (p Path) valid() error {
	if !p.IsValid() {
		return fmt.Errorf("%w: %d", ErrUnknownPath, uint8(p))
	}
	return nil
}
