// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package builtin registers the built-in strategies and presets of the support matrix,
// and declares the support matrix of the release.
//
// [Support] returns the support matrix. [Register] registers the factory of every
// built-in strategy of the matrix, and the presets that [Presets] returns. Each factory
// has a constructor of its own, such as [FixedSlots] and [Planner], so an application
// can register some built-ins, or register a built-in under another name.
//
// # Configurations
//
// The factory of a strategy parses its JSON configuration over the defaults that the
// docblock of its constructor lists. A field that the configuration sets keeps its
// value, also when the value is an empty list. Parse refuses an unknown field and data
// after the JSON value. The encoding of a parsed configuration contains every field, and
// Parse of that encoding returns an equal configuration. The seven strategies without a
// configuration share one empty configuration, which encodes as {}.
//
// The configuration of the planner selects constraints and goals, and the configuration
// of fixed-slots selects a hash function. Their factories parse each selection with
// config.ParseNamed on the registry that their constructors take, so a parsed planner
// configuration contains every default of its constraints and goals. New builds each
// selection with config.BuildNamed on the same registry.
//
// # Capabilities
//
//   - [PrimaryOnly] provides single-writer.
//   - [Exclusive] provides fenced when its configuration sets "fenced": true. It
//     requires single-writer from the replica state model, exclusive-safe from the
//     handoff protocol, and leases from the membership.
//   - [BreakBeforeMake] provides exclusive-safe.
//   - [Embedded] provides controller, and [Planner] and [Watch] require it from the
//     topology.
//
// The other strategies do not provide or require a capability.
//
// # Errors
//
// [ErrConfig] classifies as errs.Invalid. Each factory returns an error that wraps it
// for a configuration that it cannot parse or build.
//
// # Concurrency
//
// Every factory is safe for concurrent use. Each call of [Support] and [Presets] returns
// new values.
//
// # Dependency position
//
// builtin imports the standard library, the package errs of go.thesmos.sh/core, and the
// packages config, shard, partition, placement, replication, ownership and handoff of
// sharder. It also imports the built-in strategies in the subpackages of partition,
// placement and handoff.
package builtin
