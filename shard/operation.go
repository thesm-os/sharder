// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard

//go:generate go tool kanon -type=OperationID

// OperationID is the identity of one transition of one shard. Two transitions of a
// shard in one namespace incarnation never have the same OperationID, because the
// placement of the shard keeps the count of its transitions after each one ends.
//
// The zero OperationID is no transition.
//
// # Encoding
//
// kanon generates the codec of OperationID. Incarnation is field 1, Shard is field 2,
// and Sequence is field 3.
//
// # Concurrency
//
// An OperationID is a comparable value, and a copy is independent of the original.
//
// # Allocation contract
//
// Construction, comparison and a lookup in a map keyed by OperationID do not
// allocate.
type OperationID struct {
	// Incarnation is the namespace incarnation for which the controller runs the
	// transition.
	Incarnation NamespaceIncarnation
	// Shard is the shard that the transition moves.
	Shard ID
	// Sequence is the number of the transition among the transitions of the shard,
	// counted from one.
	Sequence uint64
}
