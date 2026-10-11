// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package config

//go:generate go run golang.org/x/tools/cmd/stringer@v0.52.0 -type=Axis -linecomment -output=axis.string_gen.go

// Axis is a kind of registered strategy, or the role of an adapter of the process.
//
//   - [Partitioning], [Placement], [Replication], [Ownership], [Handoff], [Topology]
//     and [Routing] are the axes of a spec. A spec has one selection on each of them.
//   - [Constraint], [Goal] and [Hash] are the axes of the strategies that the
//     factories of placers and partitioners build with BuildNamed.
//   - [Store] and [Membership] are the roles of the adapters of the process. An
//     adapter provides capabilities in its role and does not register a strategy.
//
// The zero Axis is not Valid.
//
// # Encoding
//
// [Axis.String] returns the spelling of an axis, such as "placement", and the form
// "Axis(n)" for a value that is not Valid. The JSON name of the field of an axis in a
// [Spec] is the spelling of the axis.
//
// # Concurrency
//
// An Axis is a comparable value.
type Axis uint8

const (
	// Partitioning selects the partitioner, which maps keys to positions and divides
	// the positions into shards.
	Partitioning Axis = 1 // partitioning
	// Placement selects the placer, which assigns the replicas of shards to nodes.
	Placement Axis = 2 // placement
	// Replication selects the replica state model.
	Replication Axis = 3 // replication
	// Ownership selects the ownership mode.
	Ownership Axis = 4 // ownership
	// Handoff selects the handoff protocol, which moves a replica between nodes.
	Handoff Axis = 5 // handoff
	// Topology selects where the controllers run.
	Topology Axis = 6 // topology
	// Routing selects where routers learn their routes.
	Routing Axis = 7 // routing
	// Constraint is the axis of the placement constraints that the factory of a
	// placer builds with BuildNamed.
	Constraint Axis = 8 // constraint
	// Goal is the axis of the placement goals that the factory of a placer builds
	// with BuildNamed.
	Goal Axis = 9 // goal
	// Hash is the axis of the hash functions that the factory of a partitioner
	// builds with BuildNamed.
	Hash Axis = 10 // hash
	// Store is the role of the store adapter of the process.
	Store Axis = 11 // store
	// Membership is the role of the membership adapter of the process.
	Membership Axis = 12 // membership
)

// specAxes lists the axes of a spec in the order of the fields of a Spec. Resolve and
// RegisterPreset check the selections in this order.
var specAxes = [...]Axis{Partitioning, Placement, Replication, Ownership, Handoff, Topology, Routing}

// Valid reports whether a is one of the twelve axes. The zero Axis is not.
func (a Axis) Valid() bool {
	return a >= Partitioning && a <= Membership
}
