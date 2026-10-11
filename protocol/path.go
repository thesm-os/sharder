// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package protocol

//go:generate go tool kanon -type=Path -validate=valid
//go:generate go run golang.org/x/tools/cmd/stringer@v0.52.0 -type=Path -linecomment -output=path.string_gen.go

import "fmt"

// Path is the path of the script of a transition that a callback belongs to, or the
// cleanup of an agent.
//
// The zero Path is not Valid. An encoding leaves out a field of the zero Path, so a codec
// does not check it, and the agent that receives a request checks [Path.Valid].
//
// # Encoding
//
// kanon encodes a Path as an unsigned varint of its number. A codec returns an error that
// wraps [ErrUnknownPath] for a Path other than zero that is not Valid, on encode and on
// decode. [Path.String] returns the spelling of a Path, such as "forward", and the form
// "Path(n)" for a value that is not Valid.
//
// # Concurrency
//
// A Path is a comparable value.
//
// # Allocation contract
//
// [Path.Valid], and [Path.String] of a Valid Path, do not allocate. String allocates the
// form of a value that is not Valid.
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

// Valid reports whether p is one of the three paths. The zero Path is not.
//
// # Allocation contract
//
// Zero alloc.
func (p Path) Valid() bool {
	return p >= Forward && p <= Cleanup
}

// valid returns nil for a Valid path. For any other path, it returns an error that wraps
// [ErrUnknownPath] and contains the number of p. The generated ValidateKanon calls it, so
// a codec refuses a path outside the three.
func (p Path) valid() error {
	if !p.Valid() {
		return fmt.Errorf("%w: %d", ErrUnknownPath, uint8(p))
	}
	return nil
}
