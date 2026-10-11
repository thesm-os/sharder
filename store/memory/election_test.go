// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package memory_test

import (
	"context"
	"runtime"
	"testing"
	"testing/synctest"
	"weak"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/clock/fake"

	"go.thesmos.sh/sharder/internal/streamtest"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// grant is the result of a call of Campaign on a goroutine of its own.
type grant struct {
	l   store.Leadership
	err error
}

// TestElection checks Campaign and Resign after Close, with an ended context and for a
// term that has ended. It also checks the candidates that wait, and the contexts that the
// store keeps.
func TestElection(t *testing.T) {
	t.Parallel()

	t.Run("Campaign", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the error of an ended context while no candidate leads", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			assert.HonoursCancellation(t, func(ctx context.Context) error {
				_, err := s.Campaign(ctx, scope)
				return err
			}, "Campaign returns the error of its context while no candidate leads")
		})

		t.Run("returns ErrClosed after Close", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			assert.FailsAfterClose(t, s.Close, func() error {
				_, err := s.Campaign(t.Context(), scope)
				return err
			}, memory.ErrClosed, "Campaign returns ErrClosed after Close")
		})

		t.Run("returns ErrClosed to a candidate that waits when the store closes", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				s := memory.New(fake.New(origin))
				_, err := s.Campaign(t.Context(), scope)
				assert.NoError(t, err, "Campaign grants a term")
				errc := make(chan error, 1)
				go func() {
					_, err := s.Campaign(t.Context(), scope)
					errc <- err
				}()
				synctest.Wait()
				assert.NoError(t, s.Close(), "Close returns nil")
				assert.ErrorIs(t, <-errc, memory.ErrClosed, "Campaign returns ErrClosed to a candidate that waits")
			})
		})

		t.Run("grants the term to a candidate that waits once the leader resigns", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				s := memory.New(fake.New(origin))
				t.Cleanup(func() { assert.NoError(t, s.Close(), "Close returns nil") })
				first, err := s.Campaign(t.Context(), scope)
				assert.NoError(t, err, "Campaign grants a term")
				grants := campaign(t.Context(), s)
				synctest.Wait()
				assert.NoError(t, first.Resign(t.Context()), "Resign returns nil")
				g, _ := streamtest.Receive(t, grants, waitFor, "the candidate that waits leads once the leader resigns")
				assert.NoError(t, g.err, "Campaign grants a term to the candidate that waits")
			})
		})

		t.Run("keeps the other candidates waiting when the context of one candidate ends", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				s := memory.New(fake.New(origin))
				t.Cleanup(func() { assert.NoError(t, s.Close(), "Close returns nil") })
				first, err := s.Campaign(t.Context(), scope)
				assert.NoError(t, err, "Campaign grants a term")
				grants := campaign(t.Context(), s)
				synctest.Wait()
				ended, cancel := context.WithCancel(t.Context())
				cancel()
				_, err = s.Campaign(ended, scope)
				assert.ErrorIs(t, err, context.Canceled, "Campaign returns the error of its ended context")
				assert.NoError(t, first.Resign(t.Context()), "Resign returns nil")
				g, _ := streamtest.Receive(t, grants, waitFor, "the candidate that waits leads once the leader resigns")
				assert.NoError(t, g.err, "Campaign grants a term to the candidate that waits")
			})
		})

		t.Run("releases the context of a call that ends before it leads", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			_, err := s.Campaign(t.Context(), scope)
			assert.NoError(t, err, "Campaign grants a term")
			gone := func() weak.Pointer[tracked] {
				inner, cancel := context.WithCancel(t.Context())
				cancel()
				ctx := &tracked{Context: inner}
				_, err := s.Campaign(ctx, scope)
				assert.ErrorIs(t, err, context.Canceled, "Campaign returns the error of its ended context")
				return weak.Make(ctx)
			}()
			runtime.GC()
			assert.Nil(t, gone.Value(), "the store releases the context of a call that returns without a term")
			runtime.KeepAlive(s)
		})
	})

	t.Run("Resign", func(t *testing.T) {
		t.Parallel()

		t.Run("returns nil for a term that has ended", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			l, err := s.Campaign(t.Context(), scope)
			assert.NoError(t, err, "Campaign grants a term")
			assert.NoError(t, l.Resign(t.Context()), "Resign returns nil")
			assert.NoError(t, l.Resign(t.Context()), "Resign returns nil for a term that has ended")
		})

		t.Run("returns ErrClosed after Close", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			l, err := s.Campaign(t.Context(), scope)
			assert.NoError(t, err, "Campaign grants a term")
			assert.FailsAfterClose(t, s.Close, func() error { return l.Resign(t.Context()) }, memory.ErrClosed,
				"Resign returns ErrClosed after Close")
		})

		t.Run("releases the context of the campaign", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			// The context of the test references inner until cancel runs after the check.
			// inner references each callback that the store registered on it and did not
			// stop.
			var cancel context.CancelFunc
			gone := func() weak.Pointer[tracked] {
				var inner context.Context
				inner, cancel = context.WithCancel(t.Context())
				ctx := &tracked{Context: inner}
				l, err := s.Campaign(ctx, scope)
				assert.NoError(t, err, "Campaign grants a term")
				assert.NoError(t, l.Resign(ctx), "Resign returns nil")
				return weak.Make(ctx)
			}()
			defer cancel()
			runtime.GC()
			assert.Nil(t, gone.Value(), "Resign releases the context of the campaign that granted the term")
			runtime.KeepAlive(s)
		})
	})
}

// campaign calls Campaign for the scope of the tests with ctx on a goroutine of its own,
// and returns the channel that receives the result.
func campaign(ctx context.Context, s *memory.Store) <-chan grant {
	grants := make(chan grant, 1)
	go func() {
		l, err := s.Campaign(ctx, scope)
		grants <- grant{l: l, err: err}
	}()
	return grants
}
