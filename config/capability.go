// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package config

// Capability is a property that a strategy or an adapter provides to the other axes
// of a namespace, such as [Fenced].
//
// [Capability.IsValid] refuses the zero Capability.
//
// # Encoding
//
// [Capability.String] returns the spelling of a capability, such as "single-writer",
// and the form "Capability(n)" for any other value. [ParseCapability] returns the
// capability of a spelling.
//
// # Concurrency
//
// A Capability is a comparable value.
//
//dokimi:enum text=comment
type Capability uint8

const (
	// Controller is the capability of a topology whose controllers commit the
	// assignment. A placer that plans and a routing mode that reads the committed
	// assignment require it.
	Controller Capability = 1 // controller
	// SingleWriter is the capability of a replica state model with one writer role.
	// Exclusive ownership requires it.
	SingleWriter Capability = 2 // single-writer
	// ExclusiveSafe is the capability of a handoff protocol that never runs two
	// writers of a shard at once. Exclusive ownership requires it.
	ExclusiveSafe Capability = 3 // exclusive-safe
	// Fenced is the capability of an ownership mode whose writers fence every write
	// with their epoch.
	Fenced Capability = 4 // fenced
	// Leases is the capability of a membership adapter that ends the session of a
	// node within a bounded time after the node stops renewing it. A store adapter
	// whose elections end the same way has it too. Exclusive ownership requires it
	// from the membership.
	Leases Capability = 5 // leases
	// Durable is the capability of a store adapter that persists every commit.
	Durable Capability = 6 // durable
)

// Requirement is a capability that a strategy requires from the strategy of another
// axis or from the adapter of a role.
//
// # Concurrency
//
// A Requirement is a value.
type Requirement struct {
	// Reason is the cause of the requirement. Resolve includes it in the error of a
	// requirement that a namespace does not meet.
	Reason string
	// Axis is the axis or the role whose strategy or adapter has to provide
	// Capability.
	Axis Axis
	// Capability is the required capability.
	Capability Capability
}

// Capabilities is what a strategy provides to the other axes, and what it requires
// from them.
//
// # Concurrency
//
// A copy of Capabilities shares its slices with the original.
type Capabilities struct {
	// Provides lists the capabilities that the strategy provides.
	Provides []Capability
	// Requires lists the capabilities that the strategy requires.
	Requires []Requirement
}

// Provider is the interface of the store and membership adapters. The controller
// passes the capabilities of the store adapter to Resolve under Store, and those of
// the membership adapter under Membership.
type Provider interface {
	// Provides returns the capabilities that the adapter provides, such as Leases.
	Provides() []Capability
}
