// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package protocol defines the calls between controllers and agents, the operations of
// operators and cluster managers, the metadata of every call, and the interfaces that
// transport adapters implement.
//
// A controller sends callbacks for replicas through an [AgentClient], and an agent receives
// them as an [AgentServer]. Agents send reports, operators send proposals, and cluster
// managers send disruptions through a [ControllerClient], and a controller receives them as
// a [ControllerServer]. [Dispatch] serves the callbacks of the root of a namespace and of
// its cells on one endpoint of a process. The argument of every call is one message, and
// so is its result. Every message contains its scope and its namespace incarnation, so one
// controller serves many namespaces. A receiver refuses a delayed message of a deleted
// namespace.
//
// # Versions and features
//
// Every call has [Metadata] besides its message. The metadata contains the identity of the
// caller, its major version of the protocol, and the features that the call requires. A
// receiver refuses a call of another major version before any effect. It also refuses a
// call that requires a feature that the receiver does not implement. [Metadata.Check]
// makes that check, and every server and every transport calls it before it serves a
// call. This release is major version [Current] and does not define a feature.
//
// # Deadlines
//
// A transport passes the deadline of the context of each call to the server. A server
// bounds a call that waits by that deadline, such as the deferral of a Disrupt that waits
// for the admission of its disruption.
//
// # Errors
//
// A transport keeps the class of an error across the wire. [CallError] is an error as a
// transport sends it in place of a result. It contains the class of the error under
// errs.Classify and its message.
// [ErrUnsupported] classifies as errs.Unsupported and matches errors.ErrUnsupported.
// [ErrUnreachable] classifies as errs.Transient. [ErrUnknownCall] and [ErrUnknownPath]
// classify as errs.Invalid.
//
// # Encoding
//
// kanon generates the binary codecs of [Request], [Key], [Report], [Ack], [Receipt],
// [DoneArgs], [CallError], [Call] and [Path]. A field keeps its number, and a new field
// takes a new number. Metadata has no codec, because every transport sends it in its own
// framing.
//
// # Concurrency
//
// Every implementation of [AgentServer], [ControllerServer], [AgentClient] and
// [ControllerClient] is safe for concurrent use. A server does not modify a message that
// it receives, and does not keep it after the call returns. A copy of a value of any other
// type shares its slices with the original.
//
// # Dependency position
//
// protocol imports the standard library, the packages crypto, crypto/sha256, epoch and
// errs of go.thesmos.sh/core, and the packages budget, proposal and shard of
// go.thesmos.sh/sharder. Its generated codecs also import go.thesmos.sh/kanon and
// go.thesmos.sh/kanon/wire.
package protocol
