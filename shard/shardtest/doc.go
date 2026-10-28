// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package shardtest contains the suite of the hot path shard.CellMap.Locate.
//
// [CellMapSuite] is the suite of shard.CellMap. A router locates the cell of each key that
// it routes through Locate. The budget of the suite states that Locate does not allocate,
// for every position that the call generator of Locate draws.
//
// # Dependency position
//
// shardtest imports context and testing of the standard library, the package prop of
// go.dokimi.dev/assert, go.dokimi.dev/conform, and the package shard of sharder.
package shardtest
