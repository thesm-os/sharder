// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package budget defines the planned disruptions of a cell and the budget that admits
// them.
//
// A cluster manager, an operator, or a node that stops for a deploy calls Admit on the
// [Budget] of a cell before a [Disruption] starts. The controller of the cell implements
// the Budget. It returns a [Decision], and it records each disruption that it defers or
// admits as a [Reservation] in the scope of the cell. A new controller of the cell
// continues from those reservations.
//
// # Negotiable disruptions
//
// Admit defers a negotiable disruption until the replicas have moved off its nodes and
// the limits of the namespace's spec allow it. The caller calls Admit again after the
// RetryAfter of each deferral. A disruption that is not negotiable starts at its Start
// under any Decision, and the core moves the replicas off its nodes before then.
//
// # Impacts
//
// An [Impact] is what a disruption does to the replicas on its nodes: [NetworkLoss],
// [ProcessRestart] or [FullLoss].
//
// # Encoding
//
// kanon generates the binary codecs of [Disruption], [Decision], [Reservation] and
// [Impact]. A field keeps its number, and a new field takes a new number. An [ID]
// encodes through the binary methods of the identifier that it embeds.
//
// # Errors
//
// [ErrUnknownImpact] and [ErrReusedID] classify as errs.Invalid.
//
// # Concurrency
//
// Every type of the package apart from Budget is a value type. A copy of a Disruption
// shares the elements of its Nodes with the original, and a copy of a Reservation also
// shares its map of Incarnations. An implementation of Budget is safe for concurrent
// use.
//
// # Dependency position
//
// budget imports the standard library, the packages crypto, epoch, errs and id of
// go.thesmos.sh/core, and go.thesmos.sh/sharder/shard. Its generated codecs also
// import go.thesmos.sh/kanon and go.thesmos.sh/kanon/wire.
package budget
