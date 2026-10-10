// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package fixedslots provides the partitioner fixed-slots, which hashes each key into
// one of a fixed number of slots.
//
// The slot of a key is the hash of the key modulo the number of slots. The position of
// a key is the number of its slot as 4 bytes in big-endian order. [Partitioner.Initial]
// divides a range into one span per slot, so each shard of a cell is one slot, and the
// shards never change.
//
// # Configuration
//
// A [Config] contains the registered name of the hash function, the number of slots,
// and whether keys have hash tags. The slot of a key depends on every field, so the
// mapping of the partitioner contains the whole configuration.
//
// # Hash tags
//
// With hash tags, [Partitioner.Position] hashes only the bytes between the first { of
// a key and the first } after it, when at least one byte lies between them. The keys
// user:{42}:cart and user:{42}:orders then have the same slot. Redis Cluster applies
// the same rule, so an application keeps the groups of keys that it formed for Redis
// Cluster.
//
// # Errors
//
// [ErrConfig] classifies as errs.Invalid. [Partitioner.Initial] returns an error that
// wraps partition.ErrRange, which classifies as errs.Invalid.
//
// # Concurrency
//
// A [Partitioner] is safe for concurrent use.
//
// # Allocation contract
//
// [Partitioner.Position] does not allocate when dst has room for 4 bytes.
//
// # Dependency position
//
// fixedslots imports the standard library, the package errs of go.thesmos.sh/core, and
// the packages partition and shard of sharder.
package fixedslots
