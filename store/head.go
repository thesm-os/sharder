// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package store

//go:generate go tool kanon -type=Head

import (
	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/shard"
)

// Head is the header of a scope. Every commit to the scope checks its values.
//
// The zero Head is the header of a scope before its first commit.
//
// # Encoding
//
// kanon generates the codec of Head. Incarnation is field 1, Generation is field 2,
// Watermark is field 3, and Schema is field 4.
//
// # Concurrency
//
// A Head is a comparable value, and a copy is independent of the original.
type Head struct {
	// Generation is the generation of the scope. Every commit creates the generation
	// after it.
	Generation epoch.Epoch
	// Watermark is the highest writer epoch that the scope has admitted.
	Watermark epoch.Epoch
	// Schema is the highest schema of any record or transition that the scope
	// contains. A commit raises it to the Schema of the release that writes the commit.
	Schema uint32
	// Incarnation is the namespace incarnation that created the scope. It is zero for
	// a scope before its first commit.
	Incarnation shard.NamespaceIncarnation
}
