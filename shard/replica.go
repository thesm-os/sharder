// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard

//go:generate go tool kanon -type=Replica

// Replica is one copy of a shard on one node, in one role.
//
// The zero Replica is no replica.
//
// # Encoding
//
// kanon generates the codec of Replica. Node is field 1, and Role is field 2. The
// codec returns an error that wraps [ErrUnknownRole] for a Role that is not valid.
//
// # Concurrency
//
// A Replica is a comparable value, and a copy is independent of the original.
//
// # Allocation contract
//
// Construction and comparison do not allocate.
type Replica struct {
	// Node is the node that hosts the replica.
	Node NodeID
	// Role is the role of the replica in the state model of the namespace.
	Role Role
}
