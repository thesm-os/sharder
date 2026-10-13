// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package fencedstore

import (
	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/protocol"
)

// Tx is one transaction of a Store. It reads the committed values of its store and its own
// writes. Store.Update commits its writes, its callback keys and the watermarks that its
// writers advance at once.
//
// # Concurrency
//
// A Tx is valid only during the call of Store.Update that runs it, and only on the goroutine
// of that call.
type Tx struct {
	// s is the store of the transaction.
	s *Store
	// writes maps each key that the transaction put to its value.
	writes map[string]string
	// fences maps each fence key whose watermark a writer advances to the new watermark.
	fences map[fenceKey]epoch.Epoch
	// recorded contains the key of each callback that the transaction recorded.
	recorded map[protocol.Key]struct{}
}

// Get returns the value of key in the transaction: its own write, or the committed value. It
// reports whether key has one.
func (tx *Tx) Get(key string) (string, bool) {
	if v, ok := tx.writes[key]; ok {
		return v, true
	}
	v, ok := tx.s.values[key]
	return v, ok
}

// Put sets the value of key in the transaction.
func (tx *Tx) Put(key, value string) {
	tx.writes[key] = value
}

// Recorded reports whether the transaction or a committed transaction has recorded the key
// k of a callback.
func (tx *Tx) Recorded(k *protocol.Key) bool {
	if _, ok := tx.recorded[*k]; ok {
		return true
	}
	_, ok := tx.s.recorded[*k]
	return ok
}

// Record records the key k of a callback in the transaction, with the effect of the callback.
func (tx *Tx) Record(k *protocol.Key) {
	tx.recorded[*k] = struct{}{}
}
