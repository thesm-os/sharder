// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package singleton provides the partitioner singleton, whose one shard covers every
// position of a cell.
//
// [Partitioner.Position] returns the empty position for every key, and
// [Partitioner.Initial] returns one span whose range is the range of the cell. Leader
// election uses singleton, because the one writer of the one shard is the leader.
//
// # Allocation contract
//
// [Partitioner.Position] does not allocate.
//
// # Dependency position
//
// singleton imports the packages partition and shard of sharder.
package singleton
