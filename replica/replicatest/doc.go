// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package replicatest contains the doubles of the interfaces that an application implements
// for an agent: replica.Handler, replica.Revoker, replica.LoadReporter and replica.Opener.
//
// # Doubles
//
// [HandlerStub], [RevokerStub], [LoadReporterStub] and [OpenerStub] are recording doubles of
// [replica.Handler], [replica.Revoker], [replica.LoadReporter] and [replica.Opener]. Each
// method of a double returns the results of a function, fixed results or an armed fault.
//
// # Dependency position
//
// replicatest imports the standard library, go.dokimi.dev/assert, go.dokimi.dev/stub, and the
// packages replica and shard of sharder.
package replicatest
