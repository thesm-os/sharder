// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package fencetest contains the double of fence.Acquirer.
//
// # Doubles
//
// [AcquirerStub] is a recording double of [fence.Acquirer]. Its method Acquire returns the
// result of a function, a fixed result or an armed fault.
//
// # Dependency position
//
// fencetest imports the standard library, go.dokimi.dev/assert, go.dokimi.dev/stub, and the
// package fence of sharder.
package fencetest
