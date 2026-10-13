// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package fencedstore is the storage of the applications of the examples. It is an in-memory
// key-value store whose transactions check the fence of each shard and record the key of each
// callback with its effect, and whose replicas lag behind it.
//
// # Fence
//
// [Store] implements fence.Guard over its transactions. [Store.Acquire] advances the
// watermark of the fence key of a token in one transaction. The writer that [Store.Writer]
// returns checks the fence in a transaction of [Store.Update], so a write of an older tenure
// fails with epoch.ErrFenced and does not commit.
//
// # Callbacks
//
// A handler records the key of each callback with [Tx.Record] in the transaction of the
// effect of the callback, and finds a recorded key with [Tx.Recorded]. A replayed callback
// then does not repeat its effect.
//
// # Replicas
//
// A [Replica] applies the committed transactions of its store in order. [Replica.Pause] keeps
// it at its position, and [Replica.Resume] lets it apply the rest. [Store.Reader] returns a
// replica that has applied the acquisition of a token, and the store itself for a replica that
// has not. A handler that reads through it reads the data of its shard only from a state that
// contains the acquisition.
//
// # Concurrency
//
// A Store and its replicas are safe for concurrent use. The transactions and the acquisitions
// of a store run one at a time.
//
// # Dependency position
//
// fencedstore imports the standard library, the package epoch of go.thesmos.sh/core, and the
// packages fence, protocol and shard of go.thesmos.sh/sharder.
package fencedstore
