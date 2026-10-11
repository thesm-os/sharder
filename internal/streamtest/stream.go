// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package streamtest

import (
	"context"
	"iter"
	"time"

	"go.dokimi.dev/assert"
)

// Item is one item of an iterator: the value and the error that the iterator yields
// together.
//
// # Concurrency
//
// A copy of an Item shares the memory of Value with the original.
type Item[T any] struct {
	// Err is the error that the iterator yields, or nil.
	Err error
	// Value is the value that the iterator yields.
	Value T
}

// Follow ranges over seq on a goroutine of its own, and sends each item to the channel that
// it returns. It closes the channel when the iteration ends. When ctx ends before the
// receiver takes an item, it ends the iteration. The caller ends ctx once it no longer
// receives, so the goroutine ends.
func Follow[T any](ctx context.Context, seq iter.Seq2[T, error]) <-chan Item[T] {
	items := make(chan Item[T])
	go func() {
		defer close(items)
		for v, err := range seq {
			select {
			case items <- Item[T]{Err: err, Value: v}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return items
}

// Receive returns the value that ch delivers, and reports false when ch is closed. It fails
// tb with msg when ch delivers nothing within wait, measured on the runtime clock.
func Receive[T any](tb assert.TB, ch <-chan T, wait time.Duration, msg string) (T, bool) {
	tb.Helper()
	var v T
	ok, late := false, false
	select {
	case v, ok = <-ch:
	case <-time.After(wait):
		late = true
	}
	assert.False(tb, late, msg)
	return v, ok
}

// Blocks fails tb with msg when a receive from ch would not block at the time of the call.
// Such a receive returns a buffered value, or the zero value of a closed channel.
func Blocks[T any](tb assert.TB, ch <-chan T, msg string) {
	tb.Helper()
	ready := false
	select {
	case <-ch:
		ready = true
	default:
	}
	assert.False(tb, ready, msg)
}

// End fails tb with msg when the iteration that sends to items does not end after one
// error at most. It fails tb in each of these cases:
//   - the iteration yields a value without an error.
//   - the iteration yields a second item.
//   - the iteration does not end within wait.
func End[T any](tb assert.TB, items <-chan Item[T], wait time.Duration, msg string) {
	tb.Helper()
	it, ok := Receive(tb, items, wait, msg)
	if !ok {
		return
	}
	assert.HasError(tb, it.Err, msg)
	_, ok = Receive(tb, items, wait, msg)
	assert.False(tb, ok, msg)
}
