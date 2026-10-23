// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package config

// Interruption is the interruption of service that planned work may cause. Every
// handoff protocol of the support matrix leaves a shard without a serving writer for a
// writer gap, so Resolve accepts only [PlannedInterruption].
//
// Resolve fills a zero Interruption from the preset, and refuses a spec whose preset
// does not have one.
//
// # Encoding
//
// [Interruption.String] returns the spelling of an interruption, such as "planned", and
// [ParseInterruption] returns the interruption of a spelling. The text methods write and
// read the spelling, so encoding/json writes an Interruption as a string.
//
// # Concurrency
//
// An Interruption is a comparable value.
//
//dokimi:enum text=comment
type Interruption uint8

// PlannedInterruption is the interruption under which a planned move or a planned
// disruption may leave a shard without a serving writer for the writer gap of the
// handoff protocol. The budget counts the replica of the shard as unavailable for the
// gap.
const PlannedInterruption Interruption = 1 // planned
