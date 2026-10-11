// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package protocol

import (
	"fmt"

	"go.thesmos.sh/sharder/shard"
)

// Version is a major version of the protocol. It changes only when the encoding of a
// message changes incompatibly, and a receiver refuses a call of another major version.
type Version uint32

// Current is the major version of the protocol that this release implements.
const Current Version = 1

// Feature is the registered name of a capability that a call requires of its receiver
// beyond its major version. A release that adds a field which a receiver must understand
// adds a Feature, and a caller requires it only in the calls that use the field. This
// release defines no feature.
type Feature string

// Metadata is what every call has besides its message: the identity of the caller, the
// major version of the caller, and the features that the call requires.
//
// The zero Metadata is the metadata of a caller that states no version, which every
// receiver refuses.
//
// # Concurrency
//
// A copy of a Metadata shares the elements of Requires with the original.
type Metadata struct {
	// Caller is the identity that the transport authenticated, or empty when the transport
	// authenticates no caller. A transport takes it from the connection, never from a value
	// that the caller sends.
	Caller shard.Identity
	// Requires lists the features that the call needs its receiver to implement.
	Requires []Feature
	// Version is the major version of the protocol of the caller.
	Version Version
}

// Check returns nil when this release implements the version of md and every feature that
// md requires. Every server calls it before it serves a call, and every transport calls it
// before it passes a call to a server.
//
// Error modes:
//   - an error that wraps ErrUnsupported when md.Version is not Current.
//   - an error that wraps ErrUnsupported when md.Requires lists a feature, because this
//     release implements none.
//
// # Allocation contract
//
// Zero alloc for metadata that this release implements.
func (md Metadata) Check() error {
	if md.Version != Current {
		return fmt.Errorf("%w: the call has the major version %d, and this release implements %d",
			ErrUnsupported, md.Version, Current)
	}
	if len(md.Requires) > 0 {
		return fmt.Errorf("%w: the call requires the features %q", ErrUnsupported, md.Requires)
	}
	return nil
}
