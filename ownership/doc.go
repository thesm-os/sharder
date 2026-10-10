// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package ownership configures exclusive ownership: at most one writer tenure per
// shard and epoch, with a lease that bounds how long a node writes without renewing
// it.
//
// A [Config] contains the [Lease] of the nodes of a namespace, and declares whether
// every write of the shard data of the application goes through a fence. The node
// measures its lease deadline from the send time of each renewal that the store
// accepts, and [Lease.Validity] returns how long the deadline extends past it. An
// [OnLeaseLoss] selects what the writers of a node do when the deadline passes.
//
// # Encoding
//
// encoding/json encodes a Config as an object with the keys lease and fenced, and a
// Lease as an object with the keys ttl, renew, lockDelay, maxDriftRate and onLoss.
// A duration is a Go duration string, such as "10s", and an OnLeaseLoss is its
// spelling, such as "continue-fenced".
//
// # Errors
//
// [ErrConfig] classifies as errs.Invalid.
//
// # Concurrency
//
// Every type of the package is a value type.
//
// # Dependency position
//
// ownership imports the standard library, the package errs of go.thesmos.sh/core, and
// go.thesmos.sh/sharder/shard.
package ownership
