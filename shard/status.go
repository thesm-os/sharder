// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard

//go:generate go tool kanon -type=Status -validate=valid
//go:generate go run golang.org/x/tools/cmd/stringer@v0.52.0 -type=Status -linecomment -output=status.string_gen.go

import "fmt"

// Status is the condition of a replica as its node reports it.
//
// The zero Status is not Valid. An encoding leaves out a field of the zero Status, so
// a codec does not check it, and the receiver of a report checks [Status.Valid].
//
// # Encoding
//
// kanon encodes a Status as an unsigned varint of its number. A codec returns an
// error that wraps [ErrUnknownStatus] for a Status other than zero that is not Valid,
// on encode and on decode. [Status.String] returns the spelling of a Status, such as
// "serving", and the form "Status(n)" for a value that is not Valid.
//
// # Concurrency
//
// A Status is a comparable value.
//
// # Allocation contract
//
// [Status.Valid], and [Status.String] of a Valid Status, do not allocate. String
// allocates the form of a value that is not Valid.
type Status uint8

const (
	// StatusStarting reports that a callback that adds the replica is running.
	StatusStarting Status = 1 // starting
	// StatusServing reports that the replica serves in its role.
	StatusServing Status = 2 // serving
	// StatusStopping reports that a callback that removes the replica is running.
	StatusStopping Status = 3 // stopping
	// StatusFailed reports that the last callback returned an error, or that the
	// application reported a fault in the replica.
	StatusFailed Status = 4 // failed
)

// Valid reports whether s is one of the four statuses. The zero Status is not.
//
// # Allocation contract
//
// Zero alloc.
func (s Status) Valid() bool {
	return s >= StatusStarting && s <= StatusFailed
}

// valid returns nil for a Valid status, and an error that wraps [ErrUnknownStatus]
// and states the number of s for any other. The generated ValidateKanon calls it, so
// a codec refuses a status outside the four.
func (s Status) valid() error {
	if !s.Valid() {
		return fmt.Errorf("%w: %d", ErrUnknownStatus, uint8(s))
	}
	return nil
}
