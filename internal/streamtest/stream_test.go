// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package streamtest_test

import (
	"context"
	"errors"
	"iter"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/internal/streamtest"
)

// The waits and the messages of the test.
const (
	// wait is the wait of each receive that a test expects to return at once.
	wait = 10 * time.Second
	// late is the wait of each receive that a test expects to wait in vain.
	late = time.Millisecond
	// msg is the message of each check that a test expects to fail.
	msg = "the check fails"
	// assertionFalse is the assertion of assert.False in a failure record.
	assertionFalse = "false"
	// assertionHasError is the assertion of assert.HasError in a failure record.
	assertionHasError = "err-present"
)

// The errors of the iterators of the test.
var (
	errFirst  = errors.New("streamtest_test: first error")
	errSecond = errors.New("streamtest_test: second error")
)

// TestStream checks Follow, Receive and End.
func TestStream(t *testing.T) {
	t.Parallel()

	t.Run("Follow", func(t *testing.T) {
		t.Parallel()

		t.Run("sends each item of the iterator in order", func(t *testing.T) {
			t.Parallel()
			seq := func(yield func(int, error) bool) {
				if !yield(1, nil) {
					return
				}
				yield(2, errFirst)
			}
			var got []streamtest.Item[int]
			for it := range streamtest.Follow(t.Context(), iter.Seq2[int, error](seq)) {
				got = append(got, it)
			}
			assert.Equal(t, got, []streamtest.Item[int]{{Value: 1}, {Err: errFirst, Value: 2}},
				"Follow sends each item of the iterator in order, and closes its channel after the last")
		})

		t.Run("ends the iteration once its context ends", func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			stopped := make(chan struct{})
			seq := func(yield func(int, error) bool) {
				defer close(stopped)
				for i := 0; ; i++ {
					if !yield(i, nil) {
						return
					}
				}
			}
			items := streamtest.Follow(ctx, iter.Seq2[int, error](seq))
			first, ok := streamtest.Receive(t, items, wait, "Follow sends the first item at once")
			assert.True(t, ok, "Follow sends the first item before it closes its channel")
			assert.Equal(t, first, streamtest.Item[int]{}, "Follow sends the first item of the iterator")
			cancel()
			streamtest.Receive(t, stopped, wait, "the iteration ends at once when the context of Follow ends")
			_, ok = streamtest.Receive(t, items, wait, "Follow closes its channel at once when its context ends")
			assert.False(t, ok, "Follow closes its channel once its context ends")
		})
	})

	t.Run("Receive", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the value that the channel delivers", func(t *testing.T) {
			t.Parallel()
			ch := make(chan int, 1)
			ch <- 7
			v, ok := streamtest.Receive(t, ch, wait, "Receive returns at once")
			expect.Equal(t, v, 7, "Receive returns the value that the channel delivers")
			expect.True(t, ok, "Receive reports true for a delivered value")
		})

		t.Run("reports false for a closed channel", func(t *testing.T) {
			t.Parallel()
			ch := make(chan int)
			close(ch)
			_, ok := streamtest.Receive(t, ch, wait, "Receive returns at once")
			assert.False(t, ok, "Receive reports false for a closed channel")
		})

		t.Run("fails the test when nothing arrives within the wait", func(t *testing.T) {
			t.Parallel()
			got := assert.Rejects(t, "Receive fails a channel that delivers nothing within the wait",
				func(tb assert.TB) { streamtest.Receive(tb, make(chan int), late, msg) })
			assert.Length(t, got, 1, "Receive fails once")
			expect.Equal(t, got[0].Assertion, assertionFalse, "Receive fails the check that the wait did not pass")
			expect.Equal(t, got[0].Contract, msg, "Receive fails with the message of its caller")
		})
	})

	t.Run("Blocks", func(t *testing.T) {
		t.Parallel()

		t.Run("returns for a channel without a value", func(t *testing.T) {
			t.Parallel()
			streamtest.Blocks(t, make(chan int), "Blocks passes a channel whose receive blocks")
		})

		buffered := make(chan int, 1)
		buffered <- 7
		ended := make(chan int)
		close(ended)
		tests := []struct {
			name string
			give <-chan int
		}{
			{name: "fails a channel with a buffered value", give: buffered},
			{name: "fails a closed channel", give: ended},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				got := assert.Rejects(t, "Blocks fails a channel whose receive does not block",
					func(tb assert.TB) { streamtest.Blocks(tb, tt.give, msg) })
				assert.Length(t, got, 1, "Blocks fails once")
				expect.Equal(t, got[0].Assertion, assertionFalse, "Blocks fails the check that the receive blocks")
				expect.Equal(t, got[0].Contract, msg, "Blocks fails with the message of its caller")
			})
		}
	})

	t.Run("End", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name  string
			items []streamtest.Item[int]
		}{
			{name: "returns for an iteration that ends"},
			{
				name:  "returns for an iteration that ends after one error",
				items: []streamtest.Item[int]{{Err: errFirst}},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				streamtest.End(t, closed(tt.items), wait, "End passes an iteration that ends")
			})
		}

		failures := []struct {
			name      string
			items     <-chan streamtest.Item[int]
			assertion string
		}{
			{
				name:      "fails an iteration that yields a value without an error",
				items:     closed([]streamtest.Item[int]{{Value: 1}}),
				assertion: assertionHasError,
			},
			{
				name:      "fails an iteration that yields a second item",
				items:     closed([]streamtest.Item[int]{{Err: errFirst}, {Err: errSecond}}),
				assertion: assertionFalse,
			},
			{
				name:      "fails an iteration that does not end within the wait",
				items:     make(chan streamtest.Item[int]),
				assertion: assertionFalse,
			},
		}
		for _, tt := range failures {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				got := assert.Rejects(t, "End fails an iteration that does not end after one error at most",
					func(tb assert.TB) { streamtest.End(tb, tt.items, late, msg) })
				assert.Length(t, got, 1, "End fails once")
				expect.Equal(t, got[0].Assertion, tt.assertion, "End fails the check of the item that it received")
				expect.Equal(t, got[0].Contract, msg, "End fails with the message of its caller")
			})
		}
	})
}

// closed returns a closed channel that delivers items.
func closed(items []streamtest.Item[int]) <-chan streamtest.Item[int] {
	ch := make(chan streamtest.Item[int], len(items))
	for _, it := range items {
		ch <- it
	}
	close(ch)
	return ch
}
