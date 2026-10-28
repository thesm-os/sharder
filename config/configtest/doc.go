// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package configtest contains the doubles of config.Parsed, config.Factory and
// config.Provider.
//
// # Doubles
//
// [ParsedStub], [FactoryStub] and [ProviderStub] are recording doubles of [config.Parsed],
// [config.Factory] and [config.Provider]. Each method of a double returns the results of a
// function, fixed results or an armed fault.
//
// # Dependency position
//
// configtest imports the standard library, go.dokimi.dev/assert, go.dokimi.dev/stub, and the
// packages config and shard of sharder.
package configtest
