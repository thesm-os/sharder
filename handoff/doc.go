// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package handoff turns a move of a replica into a script of steps, with an abort path
// for each step, and declares the lifecycle of a transition.
//
// A [Protocol] returns the [Script] of a [Transfer]. For each [Step] of a script, the
// engine adds or drops a replica on the node of one [Side] of the transfer, or commits
// the placement of the shard. A step runs at the epoch that the core reserved for its
// side, so a script cannot give one epoch to the nodes of two sides. The core checks
// each script with [Script.Validate] before it commits the transition. The engine then
// runs the steps in order, and runs the abort path of a step that fails.
//
// On each [Event] of the current step of a transition, the engine looks up the next
// phase of the transition in [Lifecycle].
//
// # Errors
//
// [ErrScript], [ErrTransfer], [ErrUnknownSide], [ErrUnknownEvent] and [ErrUnknownAction]
// classify as errs.Invalid.
//
// # Concurrency
//
// A Side, an Action, an Event, a Step and a Transfer are comparable values. A copy of a
// Script shares its steps with the original. Lifecycle is immutable and safe for
// concurrent use.
//
// # Allocation contract
//
// [Script.Validate] of a valid script and [Transfer.Node] do not allocate. IsValid, and
// String of a declared value, do not allocate for a [Side], an [Action] or an [Event].
//
// # Dependency position
//
// handoff imports the standard library, the packages errs and fsm of
// go.thesmos.sh/core, and go.thesmos.sh/sharder/shard.
//
//dokimi:sentinel
package handoff
