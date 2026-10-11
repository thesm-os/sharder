// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package config declares the specs of namespaces, the registry of strategies and
// presets, the support matrix, and the resolution and the change of a spec.
//
// A [Spec] selects a registered strategy and its JSON configuration on each axis of a
// namespace. A [Preset] is a partial spec with a name. A [Registry] contains the
// factories of strategies and the presets, with the [Support] matrix of the release.
//
// [Resolve] fills a spec from its preset and the defaults, and parses every selection
// with its factory. It checks the result against the support matrix and against the
// requirements across the axes. [Change] returns nil when a running namespace may move
// to a new resolved spec by a spec edit alone.
//
// # Strategies
//
// An application registers a strategy through a [Factory] of the interface of its
// axis, such as a factory of partition.Partitioner on [Partitioning]. The factory
// parses and validates the configuration of the strategy. It also returns the
// [Capabilities] that the strategy provides and requires with that configuration. The
// strategy packages do not import config, so an application uses a strategy without a
// registry. No package registers anything from an init function.
//
// # Merging over a preset
//
// Resolve merges the configuration of a selection over the configuration of the preset
// when the selection has the strategy of the preset. The merge is the JSON Merge Patch
// of RFC 7396. It merges two objects member by member, and deletes a member whose value
// in the patch is null. Every other value of the patch replaces the value of the
// preset, so an array replaces the whole array of the preset.
//
// Resolve starts a selection of another strategy from the defaults of that strategy.
// The root stores the resolved spec with the defaults of every configuration, so a
// release that changes a default does not change a running namespace.
//
// # Errors
//
//   - [ErrDuplicate], [ErrUnknown], [ErrSpec], [ErrRegistration] and [ErrChange]
//     classify as errs.Invalid.
//   - [ErrUnsupported] classifies as errs.Unsupported, and errors.Is matches it with
//     errors.ErrUnsupported.
//
// Resolve returns every problem of one class in one error. When a spec has problems
// with the support matrix, Resolve returns those problems alone.
//
// # Concurrency
//
// A [Registry] and every [Factory] are safe for concurrent use. A [Resolved] is safe
// for concurrent reads. A copy of a value of any other type shares its slices and its
// pointers with the original.
//
// # Dependency position
//
// config imports the standard library, the packages epoch and errs of
// go.thesmos.sh/core, and the packages shard, partition and replication of
// go.thesmos.sh/sharder.
package config
