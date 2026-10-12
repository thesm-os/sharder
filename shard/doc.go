// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package shard defines the value types that every sharder package shares.
//
// A namespace maps each key to a position, divides the positions into cells, and
// divides each cell into shards. The nodes of a cell host the replicas of its shards.
// The types of this package describe these parts. Every other package of sharder
// imports them, and shard does not import another package of sharder.
//
// # Identities
//
// The core assigns the identities of namespace incarnations and shards with core's
// identifiers. [NamespaceIncarnation] embeds a [go.thesmos.sh/core/id.ID], and [ID]
// embeds the [go.thesmos.sh/core/id.ID128] of a ULID, so a map keyed by ID hashes and
// compares 16 bytes. A cell is a shard of the root, and the root gives the
// cell the canonical text of that shard's ID as its [CellID]. Applications, operators
// and transports choose the names of namespaces, nodes and processes. [Namespace],
// [NodeID], [CellID] and [Identity] are string types, because node labels and messages
// contain them as text. Each strategy package declares its registered name as a
// [StrategyName].
//
// # Durations
//
// A configuration writes a span of time as a [Duration]. Its JSON form is a Go
// duration string, such as "30s".
//
// # Enumerations
//
// [Role], [Status], [Fault] and [Phase] are closed enumerations of uint8. Each has a String
// method that returns the spelling of a value in configurations and telemetry, and a
// Valid method that reports whether a value is one of its constants. The numbers of
// the constants are persisted: a constant keeps its number, and a new constant takes
// the next number. [RoleCount] is the number of roles, so an array with one element
// per role has RoleCount elements.
//
// # Encoding
//
// kanon generates the binary codecs of [Scope], [OperationID], [Mapping], [Range],
// [Replica], [Placement], [Node], [Cell] and [Observed]. The messages of the protocol
// and the records of the store encode these types through the codecs, so this
// package records their field numbers once. Each field keeps its number, and a new
// field takes a new number. A process of an older release decodes a message of a
// newer one, because a decode skips the fields that it does not know.
//
// The codecs return an error for a [Role], a [Status], a [Fault] or a [Phase] that is
// not Valid, on encode and on decode. An encoding leaves out every field of a zero
// value, so a codec does not check the zero [Status] or the zero [Phase], which are not
// Valid. [NamespaceIncarnation] encodes through the binary methods of the identifier
// that it embeds. [ID] encodes as the bytes that those methods write for its ULID.
//
// # Errors
//
// [ErrIncarnation] classifies as errs.Conflict. [ErrUnknownRole], [ErrUnknownStatus],
// [ErrUnknownFault], [ErrUnknownPhase], [ErrDuration] and [ErrID] classify as
// errs.Invalid.
//
// # Concurrency
//
// Every type of the package is a value type. A copy of a value that has a slice or a
// map shares the elements with the original, so concurrent readers do not need a lock
// while every goroutine only reads. A published [Node] or [Load] is never modified.
//
// # Allocation contract
//
// [Range.Contains], [CellMap.Locate], and the Valid and String methods of the
// enumerations do not allocate for a valid value. The methods of the codecs follow
// the allocation contract of [go.thesmos.sh/kanon.Message].
//
// # Dependency position
//
// shard imports the standard library and the packages epoch, errs, id, id/ulid and tag
// of go.thesmos.sh/core. Its generated codecs also import go.thesmos.sh/kanon and
// go.thesmos.sh/kanon/wire.
package shard
