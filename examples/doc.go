// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package examples derives the catalogue of the examples of sharder from its support matrix.
//
// Each example is a program in a directory of its own. The key of the directory is the kind of
// the example and the registered name that the example shows, such as preset-exclusive-worker.
// [Keys] returns the key of each example that the support matrix requires. The tests of the
// package check the directories of the module and the table of its README.md against them.
//
// # Kinds
//
// The kinds are the terms of a spec. [KindPreset] covers each preset that builtin.Presets
// returns, and [KindTopology] covers each topology that builtin.Support lists.
//
// # Dependency position
//
// examples imports the standard library and the packages builtin and config of
// go.thesmos.sh/sharder.
package examples
