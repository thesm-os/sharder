// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard

//go:generate go tool kanon -type=Status -validate=valid

import "fmt"

// Status is the condition of a replica as its node reports it.
//
// [Status.IsValid] refuses the zero Status. An encoding leaves out a field of the zero
// Status, so a codec does not check it, and the receiver of a report checks
// [Status.IsValid].
//
// # Encoding
//
// kanon encodes a Status as an unsigned varint of its number. A codec returns an error
// that wraps [ErrUnknownStatus] for a Status other than zero outside the four statuses,
// on encode and on decode. [Status.String] returns the spelling of a Status, such as
// "serving", and the form "Status(n)" for any other value. [ParseStatus] returns the
// Status of a spelling.
//
// # Concurrency
//
// A Status is a comparable value.
//
// # Allocation contract
//
// [Status.IsValid], and [Status.String] of one of the four statuses, do not allocate.
// String allocates the form of any other value.
//
//dokimi:enum text=comment
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

// valid returns nil for one of the four statuses, and an error that wraps
// [ErrUnknownStatus] and states the number of s for any other. The generated
// ValidateKanon calls it, so a codec refuses a status outside the four.
func (s Status) valid() error {
	if !s.IsValid() {
		return fmt.Errorf("%w: %d", ErrUnknownStatus, uint8(s))
	}
	return nil
}
