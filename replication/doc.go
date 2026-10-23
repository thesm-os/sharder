// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package replication declares replica state models. A model has the roles that the
// replicas of a shard take, each with a bound on its replicas, and the transitions
// between the roles.
//
// A [Model] lists its serving roles from the enumeration shard.Role and the
// transitions between them. [Model.Compile] validates a model and builds its
// transition table with core's fsm, with shard.Role as the state and the event type.
// Compile starts the table in shard.RoleOffline and ends it in shard.RoleDropped, and
// only shard.RoleOffline leads to shard.RoleDropped. The core checks [Compiled.Spec]
// before it commits a change of role, and starts the pending transitions of a shard
// in ascending priority, within the bound of every role.
//
// # Built-in models
//
//   - [PrimaryOnly] gives each shard one replica, the primary, which writes.
//
// # Errors
//
// [ErrInvalidModel] classifies as errs.Invalid.
//
// # Concurrency
//
// A [Compiled] is immutable and safe for concurrent use. A [Model] is a value, and a
// caller must not modify one that another goroutine compiles.
//
// # Dependency position
//
// replication imports the standard library, the packages errs and fsm of
// go.thesmos.sh/core, and go.thesmos.sh/sharder/shard.
//
//dokimi:sentinel
package replication
