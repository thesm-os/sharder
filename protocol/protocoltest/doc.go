// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package protocoltest checks a transport of package protocol against the laws of the
// protocol.
//
// [AssertTransport] runs one subtest per method of the clients of a transport. Its servers
// record each call that they receive, and properties over generated messages check that
// the transport passes each message and the metadata of the caller to the server, returns
// the result of the server, keeps the class of an error of the server, and refuses a call
// of another major version or one that requires a feature before the server receives it.
//
// # Concurrency
//
// AssertTransport runs its subtests in parallel, and each case connects clients and
// servers of its own, so no case observes the calls of another.
//
// # Dependency position
//
// protocoltest imports the standard library, go.dokimi.dev/assert and its package prop, the
// packages epoch, errs and id of go.thesmos.sh/core, and the packages budget, proposal,
// protocol and shard of go.thesmos.sh/sharder.
package protocoltest
