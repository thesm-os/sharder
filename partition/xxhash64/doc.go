// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package xxhash64 provides the hash xxhash64, which computes XXH64 with seed 0 over
// the bytes of a key.
//
// [Hash] calls github.com/cespare/xxhash/v2, which computes XXH64 in assembly on amd64
// and arm64. The sums of Hash equal the XXH64 vectors of the sanity check of the
// reference implementation of xxHash.
//
// # Allocation contract
//
// [Hash.Sum64] does not allocate.
//
// # Dependency position
//
// xxhash64 imports github.com/cespare/xxhash/v2, and the packages partition and shard
// of sharder.
package xxhash64
