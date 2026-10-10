// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard

//go:generate go tool kanon -type=Scope

// Scope is the address of one keyspace of a namespace in the store. The keyspace is
// the root of the namespace when Cell is empty, and one of the namespace's cells
// otherwise. Every store call and every protocol call takes the scope that it acts
// on.
//
// The zero Scope is no keyspace.
//
// # Encoding
//
// kanon generates the codec of Scope. Namespace is field 1, and Cell is field 2.
//
// # Concurrency
//
// A Scope is a comparable value, and a copy is independent of the original.
//
// # Allocation contract
//
// Construction and comparison do not allocate.
type Scope struct {
	// Namespace is the namespace whose keyspace the scope addresses.
	Namespace Namespace
	// Cell is the cell whose keyspace the scope addresses, or empty for the root of
	// the namespace.
	Cell CellID
}
