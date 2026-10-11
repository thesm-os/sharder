// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package streamtest receives the items of iterators and channels for the tests and the
// conformance kits of sharder, with a bound on each wait.
//
// A watch of a store or of a membership blocks until its context ends. [Follow] ranges over
// such an iterator on a goroutine of its own, and sends each item to a channel. [Receive]
// takes one value from a channel and fails the test when none arrives within its wait, so a
// kit reports a store that never yields instead of waiting for the timeout of go test.
// [Blocks] checks that a channel has nothing to deliver, and [End] checks that an iteration
// ends.
//
// # Concurrency
//
// Follow starts one goroutine, which ends when the iteration ends or when the context of
// Follow ends. Receive and End block the calling goroutine for at most their wait per
// receive.
//
// # Dependency position
//
// streamtest imports the standard library and go.dokimi.dev/assert.
package streamtest
