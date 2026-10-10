// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package partitiontest checks a partition.Partitioner and a partition.Hash against
// the laws of package partition.
//
// An implementation calls [AssertPartitioner] or [AssertHash] in the test function
// Test<Unit>Conformance. The tests of its own behaviour are in other test functions,
// so their subtests and the subtests of the laws have different names. Each law is a
// parallel subtest under the name of the method that it constrains, such as Position.
//
// # Laws of a partitioner
//
//   - Position returns the same position for the same key, and appends the position
//     to dst.
//   - Mapping returns an equal mapping on every call. The mapping contains the name of
//     the partitioner and its configuration as JSON. It contains a hash version
//     exactly when it contains the name of a hash.
//   - Initial returns spans in position order that partition the position space,
//     without gaps or overlaps. It also partitions every range between the bounds of
//     two of those spans, because a cell starts and ends at the bounds of shards. It
//     returns equal spans on every call.
//
// # Laws of a hash
//
//   - Sum64 returns the sum of each vector that the caller states. It returns the
//     same hash for the same key.
//   - Version returns the version that the caller states, which is at least one.
//
// # Allocations
//
// The laws do not count allocations. A count of allocations covers the whole process,
// and the laws run as parallel subtests. The package of an implementation checks its
// allocation contract in a test of its own.
//
// # Dependency position
//
// partitiontest imports the standard library, go.dokimi.dev/assert with its packages
// expect and prop, and the packages partition and shard of sharder.
package partitiontest
