// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package store

//go:generate go tool kanon -type=Mutation,Applied

import (
	"fmt"

	"go.thesmos.sh/core/crypto"
	"go.thesmos.sh/core/crypto/sha256"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/shard"
)

// Mutation is the set of changes that one commit applies to a scope.
//
// The zero Mutation does not change the placements or the state of a scope. Its commit
// creates a generation.
//
// # Encoding
//
// kanon generates the codec of Mutation. ID is field 1, Put is field 2, Transitions is
// field 3, Done is field 4, Cell is field 5, Mapping is field 6, Spec is field 7,
// Applied is field 8, Forget is field 9, Reserve is field 10, Release is field 11,
// Drain is field 12, Undrain is field 13, and Confirm is field 14. The codec returns the
// error of the codec of a field whose value fails to encode, such as a placement with a
// replica whose role is not Valid.
//
// # Concurrency
//
// A copy of a Mutation shares its slices, the values of its pointers and the bytes of
// Spec with the original.
type Mutation struct {
	// Cell replaces the controller record of the scope. Nil leaves it unchanged.
	Cell *shard.Cell
	// Mapping sets the mapping of the namespace, in the commit that creates the root
	// scope. Every later commit leaves it nil.
	Mapping *shard.Mapping
	// Spec replaces the spec of the root scope. Nil leaves it unchanged.
	Spec []byte
	// Done lists the shards whose transition ended. The commit ends them before it
	// applies Transitions.
	Done []shard.ID
	// Transitions lists the transitions that the commit creates or advances. A
	// transition replaces the transition of its shard.
	Transitions []Transition
	// Put lists the placements that the commit creates or replaces.
	Put []shard.Placement
	// Applied records the proposals that the commit applies.
	Applied []Applied
	// Forget removes the applied proposals whose Expires has passed, after Applied.
	Forget []proposal.ID
	// Reserve adds reservations, or replaces the reservations with the same IDs, such
	// as a pending reservation that the budget admits.
	Reserve []budget.Reservation
	// Release removes the reservations with these IDs, after Reserve.
	Release []budget.ID
	// Drain adds drains, or replaces the drains with the same keys.
	Drain []Drain
	// Undrain removes the drains with these keys, after Drain. Removing one drain of a
	// node leaves the other drains of the node in effect.
	Undrain []DrainKey
	// Confirm removes the receipts of earlier commits of the writer whose outcome the
	// writer knows.
	Confirm []CommitID
	// ID is the identity of the commit. A retry of the same mutation reuses it.
	ID CommitID
}

// Digest returns the SHA-256 digest of the encoding of m. A store records the digest
// in the receipt of the commit, and refuses a commit whose ID it has recorded with
// another digest.
//
// Error modes:
//   - an error that wraps the error of the codec of m when m fails to encode.
func (m *Mutation) Digest() (crypto.Digest, error) {
	b, err := m.AppendBinary(nil)
	if err != nil {
		return crypto.Digest{}, fmt.Errorf("store: digest of mutation %v: %w", m.ID, err)
	}
	return sha256.Hasher{}.Hash(b), nil
}

// Applied is one proposal that a commit applies, with the identity that submitted it.
//
// The zero Applied is no proposal.
//
// # Encoding
//
// kanon generates the codec of Applied. Proposal is field 1, and Caller is field 2.
//
// # Concurrency
//
// A copy of an Applied shares the bytes of the Body of its Proposal with the original.
type Applied struct {
	// Caller is the identity that submitted the proposal, as the transport adapter
	// authenticated it.
	Caller shard.Identity
	// Proposal is the applied proposal. Its ID and its Expires are the entry that
	// State.Applied keeps.
	Proposal proposal.Proposal
}
