// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package handofftest contains the double of handoff.Protocol.
//
// # Doubles
//
// [ProtocolStub] is a recording double of [handoff.Protocol]. Each method of the double
// returns the results of a function, fixed results or an armed fault.
//
// # Dependency position
//
// handofftest imports the standard library, go.dokimi.dev/assert, go.dokimi.dev/stub, and the
// packages handoff and shard of sharder.
package handofftest
