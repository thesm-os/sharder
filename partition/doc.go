// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package partition maps the keys of a namespace to positions, and divides the range
// of a new cell into shards.
//
// A [Partitioner] gives each key a position. Positions are byte strings in the order
// of bytes.Compare. A Partitioner divides the positions of a new cell into a [Span]
// per shard, and the core gives each span the ID of a new shard when it commits the
// first placement of the shard. A [Hash] is the function that a hashing partitioner
// applies to a key.
//
// A [Layout] lists the shards of one cell, each a [Shard] with the ID that the core
// assigned. [Layout.Locate] returns the ID of the shard whose range contains a
// position.
//
// # Implementations
//
//   - go.thesmos.sh/sharder/partition/fixedslots is the partitioner fixed-slots, which
//     hashes each key into one of a fixed number of slots.
//   - go.thesmos.sh/sharder/partition/singleton is the partitioner singleton, whose one
//     shard covers every position of a cell.
//   - go.thesmos.sh/sharder/partition/xxhash64 is the hash xxhash64, which is XXH64
//     with seed 0.
//   - go.thesmos.sh/sharder/partition/partitiontest checks a partitioner and a hash
//     against the laws of this package.
//
// # Errors
//
// [ErrRange] classifies as errs.Invalid.
//
// # Concurrency
//
// A Partitioner and a Hash are safe for concurrent use. A Span, a Shard and a Layout
// are value types.
//
// # Allocation contract
//
// [Partitioner.Position] does not allocate when dst has room for the position.
// [Hash.Sum64] and [Layout.Locate] do not allocate.
//
// # Dependency position
//
// partition imports the standard library, the packages errs and tag of
// go.thesmos.sh/core, and go.thesmos.sh/sharder/shard.
//
//dokimi:sentinel
package partition
