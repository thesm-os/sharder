// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package proposal defines the changes that operators propose to a namespace.
//
// Every change that an operator requests is a [Proposal]. The protocol sends it to the
// controller that applies it. A proposal contains the scope of that controller and the
// namespace incarnation that it changes, so a controller that serves many namespaces
// applies it to the right one. The controller refuses a delayed proposal of a deleted
// namespace.
//
// A controller refuses a proposal in three cases:
//
//   - Its Expires has passed.
//   - Its scope has applied its [ID].
//   - Its Base is not the version of the state that it changes.
//
// # Kinds
//
// The root applies [SpecChange], which replaces the spec. The controller of a cell
// applies [Move], [Drain], [Undrain] and [Explain], which act on the shards and the
// nodes of the cell.
//
// # Encoding
//
// kanon generates the binary codecs of [Proposal] and [OperationKind]. A field keeps
// its number, and a new field takes a new number. An [ID] encodes through the binary
// methods of the identifier that it embeds.
//
// # Errors
//
// [ErrUnknownKind] classifies as errs.Invalid.
//
// # Concurrency
//
// Every type of the package is a value type. A copy of a Proposal shares the bytes of
// its Body with the original.
//
// # Dependency position
//
// proposal imports the standard library, the packages epoch, errs and id of
// go.thesmos.sh/core, and go.thesmos.sh/sharder/shard. Its generated codecs also
// import go.thesmos.sh/kanon and go.thesmos.sh/kanon/wire.
package proposal
