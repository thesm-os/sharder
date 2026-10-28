// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package drivertest contains the doubles of the interfaces of the application that the
// driver calls: driver.Handler, driver.Revoker and driver.LoadReporter.
//
// # Doubles
//
// [HandlerStub], [RevokerStub] and [LoadReporterStub] are recording doubles of
// [driver.Handler], [driver.Revoker] and [driver.LoadReporter]. Each method of a double
// returns the results of a function, fixed results or an armed fault.
//
// # Dependency position
//
// drivertest imports the standard library, go.dokimi.dev/assert, go.dokimi.dev/stub, and the
// packages fence, internal/driver, protocol and shard of sharder.
package drivertest
