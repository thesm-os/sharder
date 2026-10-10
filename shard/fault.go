// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard

//go:generate go tool kanon -type=Fault -validate=valid
//go:generate go run golang.org/x/tools/cmd/stringer@v0.52.0 -type=Fault -linecomment -output=fault.string_gen.go

import "fmt"

// Fault is the class of a fault that an application reports in one of its replicas.
//
// The zero Fault is [FaultNone], the fault of a replica without a reported fault.
//
// # Encoding
//
// kanon encodes a Fault as an unsigned varint of its number. A codec returns an error
// that wraps [ErrUnknownFault] for a Fault that is not Valid, on encode and on
// decode. [Fault.String] returns the spelling of a Fault, such as "transient", and
// the form "Fault(n)" for a value that is not Valid.
//
// # Concurrency
//
// A Fault is a comparable value.
//
// # Allocation contract
//
// [Fault.Valid], and [Fault.String] of a Valid Fault, do not allocate. String
// allocates the form of a value that is not Valid.
type Fault uint8

const (
	// FaultNone reports that the application has not reported a fault in the
	// replica.
	FaultNone Fault = 0 // none
	// FaultTransient reports a condition that stops the replica from serving until
	// the application reports that it recovered.
	FaultTransient Fault = 1 // transient
	// FaultPermanent reports that the replica cannot serve again. The core replaces
	// it on another node. In a delegated namespace, the authority of the application
	// replaces it, and the core counts it as unavailable until the application
	// releases it.
	FaultPermanent Fault = 2 // permanent
)

// Valid reports whether f is one of the three faults, [FaultNone] included.
//
// # Allocation contract
//
// Zero alloc.
func (f Fault) Valid() bool {
	return f <= FaultPermanent
}

// valid returns nil for a Valid fault, and an error that wraps [ErrUnknownFault] and
// states the number of f for any other. The generated ValidateKanon calls it, so a
// codec refuses a fault outside the three.
func (f Fault) valid() error {
	if !f.Valid() {
		return fmt.Errorf("%w: %d", ErrUnknownFault, uint8(f))
	}
	return nil
}
