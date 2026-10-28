// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package machinetest contains the double of machine.Machine.
//
// # Doubles
//
// [MachineStub] is a recording double of [machine.Machine]. Each method of the double
// returns the results of a function, fixed results or an armed fault.
//
// # Dependency position
//
// machinetest imports the standard library, go.dokimi.dev/assert, go.dokimi.dev/stub, and the
// package internal/machine of sharder.
package machinetest
