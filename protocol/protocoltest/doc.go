// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package protocoltest contains the conformance suite of the transports of package
// protocol, and the doubles of the clients of the protocol.
//
// The suite of a transport is the suite of a [Connector] over the transport, because each
// check chooses the metadata of its calls before the transport connects. [RunConnectorSuite]
// applies every check of [ConnectorSuite] to a new connector of a factory, one connector
// for each check. Each check draws one method of the clients: Do, Report, Submit, Disrupt
// or Done. Its servers record each call that they receive, and properties over generated
// messages check four laws:
//
//   - The transport passes each message and the metadata of the caller to the server, and
//     returns the result of the server.
//   - The client returns an error of the class of the error of the server.
//   - The transport refuses a call of another major version before the server receives it.
//   - The transport refuses a call that requires a feature before the server receives it.
//
// [MetadataSuite] is the suite of the hot path protocol.Metadata.Check, which every server
// and every transport calls for every call. Its budget states that Check of the metadata
// of a call that this release implements does not allocate.
//
// # Planted defects
//
// Each check has a planted defect: the double of Connector over a local network of package
// protocol/local, which changes the metadata of the calls or the errors of the clients
// that break the law of the check. The test of the suite runs each check on its defect,
// and the check must fail at the assertion whose message is its statement. Every other
// check must pass on the defect.
//
// # Doubles
//
// [ConnectorStub], [AgentClientStub] and [ControllerClientStub] are recording doubles of
// [Connector], [protocol.AgentClient] and [protocol.ControllerClient]. Each method of a
// double returns the results of a function, fixed results or an armed fault.
//
// # Concurrency
//
// The checks run in parallel, and each case connects clients and servers of its own, so no
// case observes the calls of another.
//
// # Dependency position
//
// protocoltest imports the standard library, go.dokimi.dev/assert with its package prop,
// go.dokimi.dev/conform, go.dokimi.dev/stub, the packages epoch, errs and id of
// go.thesmos.sh/core, and the packages budget, proposal, protocol, protocol/local and
// shard of sharder.
package protocoltest
