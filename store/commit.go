// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package store

//go:generate go tool kanon -type=CommitID,Receipt

import (
	"go.thesmos.sh/core/crypto"
	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/shard"
)

// CommitID is the identity of one commit to a scope. A writer numbers its commits from
// one in Seq, and no two writers share a term, so no two commits of a scope have one
// ID. A retry of a commit reuses its ID. A commit ID differs from the operation ID of
// the transition that the commit advances.
//
// The zero CommitID is no commit.
//
// # Encoding
//
// kanon generates the codec of CommitID. Incarnation is field 1, Term is field 2, and
// Seq is field 3.
//
// # Concurrency
//
// A CommitID is a comparable value, and a copy is independent of the original.
type CommitID struct {
	// Incarnation is the namespace incarnation of the writer.
	Incarnation shard.NamespaceIncarnation
	// Term is the fence epoch of the writer.
	Term epoch.Epoch
	// Seq is the number of the commit among the commits of the writer, from one.
	Seq uint64
}

// Receipt is the record of one applied commit. The store writes it in the same
// transaction as the mutation of the commit.
//
// The zero Receipt is no receipt.
//
// # Encoding
//
// kanon generates the codec of Receipt. ID is field 1, Digest is field 2, and
// Generation is field 3.
//
// # Concurrency
//
// A Receipt is a comparable value, and a copy is independent of the original.
type Receipt struct {
	// ID is the identity of the commit.
	ID CommitID
	// Digest is the digest of the mutation of the commit, which Mutation.Digest
	// returns.
	Digest crypto.Digest
	// Generation is the generation that the commit created.
	Generation epoch.Epoch
}
