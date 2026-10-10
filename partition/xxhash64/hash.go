// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package xxhash64

import (
	"github.com/cespare/xxhash/v2"

	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/shard"
)

// Name is the registered name of the hash, which the configuration of a partitioner
// and the mapping of a namespace contain.
const Name shard.StrategyName = "xxhash64"

// Hash is the hash xxhash64. It computes XXH64 with seed 0 over the bytes of a key,
// and its version is 1. The zero Hash is ready for use.
//
// # Concurrency
//
// A Hash has no state and is safe for concurrent use.
//
// # Allocation contract
//
// [Hash.Sum64] does not allocate.
type Hash struct{}

var _ partition.Hash = Hash{}

// Sum64 returns XXH64 of key with seed 0.
func (Hash) Sum64(key []byte) uint64 {
	return xxhash.Sum64(key)
}

// Version returns 1, the version of the function that Sum64 computes.
func (Hash) Version() uint32 {
	return 1
}
