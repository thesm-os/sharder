// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package fence defines fencing tokens and the interfaces of fenced storage. Every
// implementation applies core's fence laws.
//
// The application implements a [Guard] over its own storage, because the check of the
// fence has to run in the same transaction as the writes of the application. A writer
// acquires the fence with its [Token] before it reads the data of its shard, and a
// [Writer] bound to the token checks the fence in every transaction that writes the
// data:
//
//   - A write whose epoch is at or above the watermark of its fence key is admitted,
//     and a greater epoch advances the watermark.
//   - A write at epoch.Zero is unfenced. It is admitted and leaves the watermark
//     unchanged.
//   - A write below the watermark returns epoch.ErrFenced, which classifies as
//     errs.Conflict. The writer abdicates instead of retrying.
//
// # Implementations
//
// An implementation keeps one watermark per fence key, and an absent record is a
// watermark of zero. It passes core's coretest/epochtest.AssertFencedWriter with two
// named writes. One write is the acquisition, and the other is a transaction that
// checks the fence and writes one row.
//
// # Dependency position
//
// fence imports the standard library, the package epoch of go.thesmos.sh/core, and
// go.thesmos.sh/sharder/shard.
package fence
