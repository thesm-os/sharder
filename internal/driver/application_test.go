// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package driver_test

import (
	"context"
	"testing"
	"testing/synctest"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/internal/driver"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/protocol"
)

// handler is a driver.Handler whose methods call its functions.
type handler struct {
	add  func(ctx context.Context, r *protocol.Request) error
	drop func(ctx context.Context, r *protocol.Request) error
}

var _ driver.Handler = (*handler)(nil)

// AddShard calls the add function of h.
func (h *handler) AddShard(ctx context.Context, r *protocol.Request) error {
	return h.add(ctx, r)
}

// DropShard calls the drop function of h.
func (h *handler) DropShard(ctx context.Context, r *protocol.Request) error {
	return h.drop(ctx, r)
}

// acquirer is a fence.Acquirer whose Acquire calls the function.
type acquirer func(ctx context.Context, t fence.Token) error

var _ fence.Acquirer = acquirer(nil)

// Acquire calls f.
func (f acquirer) Acquire(ctx context.Context, t fence.Token) error {
	return f(ctx, t)
}

// The names of the methods of a handler, which the tests record for each call.
const (
	addShard  = "AddShard"
	dropShard = "DropShard"
)

// fencing is the fencing token of the acquisitions of the tests.
var fencing = fence.Token{Namespace: namespace, Incarnation: inc, Shard: shardA, Epoch: 3}

// TestApplication checks the effects of the application that Run performs: Handle and
// Acquire.
func TestApplication(t *testing.T) {
	t.Parallel()

	t.Run("Run", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			give protocol.Call
			name string
			want string
		}{
			{name: "calls AddShard for a Handle of an add", give: protocol.Add, want: addShard},
			{name: "calls DropShard for a Handle of a drop", give: protocol.Drop, want: dropShard},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					r := &protocol.Request{Call: tt.give}
					f := newFixture(t, performing(machine.Handle{Request: r, Token: 1}))
					var method string
					var gotRequest *protocol.Request
					f.cfg.Handler = &handler{
						add: func(_ context.Context, req *protocol.Request) error {
							method, gotRequest = addShard, req
							return errBusy
						},
						drop: func(_ context.Context, req *protocol.Request) error {
							method, gotRequest = dropShard, req
							return errBusy
						},
					}
					got, ok := f.results(t, 1)[0].(machine.Handled)
					assert.True(t, ok, "the run must pass a Handled")
					expect.That(t, got.Err).ErrorIs(errBusy, "the Handled must have the error of the handler")
					expect.Equal(t, got.Token, machine.Token(1), "the Handled must have the token of the Handle")
					expect.Equal(t, method, tt.want, "the run must call the method of the call of the request")
					expect.Equal(t, gotRequest, r, "the run must pass the request of the Handle", assert.ByIdentity())
				})
			})
		}

		t.Run("passes ErrUnsupported for a Handle of a call that is not valid", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Handle{Request: &protocol.Request{}, Token: 1}))
				f.cfg.Handler = &handler{}
				got, ok := f.results(t, 1)[0].(machine.Handled)
				assert.True(t, ok, "the run must pass a Handled")
				expect.That(t, got.Err).ErrorIs(protocol.ErrUnsupported, "the Handled must have ErrUnsupported")
				expect.Equal(t, got.Token, machine.Token(1), "the Handled must have the token of the Handle")
			})
		})

		t.Run("ends a Handle at its Timeout", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(
					t,
					performing(
						machine.Handle{Request: &protocol.Request{Call: protocol.Add}, Token: 1, Timeout: timeout},
					),
				)
				f.cfg.Handler = &handler{
					add: func(ctx context.Context, _ *protocol.Request) error { return blocked(ctx) },
				}
				got, ok := f.results(t, 1)[0].(machine.Handled)
				assert.True(t, ok, "the run must pass a Handled")
				assert.ErrorIs(
					t,
					got.Err,
					context.DeadlineExceeded,
					"the Handled must have the error of the ended call",
				)
			})
		})

		t.Run("passes ErrConfig for a Handle without a handler", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Handle{Request: &protocol.Request{Call: protocol.Add}, Token: 1}))
				got, ok := f.results(t, 1)[0].(machine.Handled)
				assert.True(t, ok, "the run must pass a Handled")
				expect.That(t, got.Err).ErrorIs(driver.ErrConfig, "the Handled must have ErrConfig")
				expect.Equal(t, got.Token, machine.Token(1), "the Handled must have the token of the Handle")
			})
		})

		t.Run("passes the fencing token of an Acquire to the guard", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Acquire{Fence: fencing, Token: 1}))
				var gotToken fence.Token
				f.cfg.Guard = acquirer(func(_ context.Context, ft fence.Token) error {
					gotToken = ft
					return errBusy
				})
				got, ok := f.results(t, 1)[0].(machine.Acquired)
				assert.True(t, ok, "the run must pass an Acquired")
				expect.That(t, got.Err).ErrorIs(errBusy, "the Acquired must have the error of the guard")
				expect.Equal(t, got.Token, machine.Token(1), "the Acquired must have the token of the Acquire")
				expect.Equal(t, gotToken, fencing, "the run must acquire the fencing token of the Acquire")
			})
		})

		t.Run("ends an Acquire at its Timeout", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Acquire{Fence: fencing, Token: 1, Timeout: timeout}))
				f.cfg.Guard = acquirer(func(ctx context.Context, _ fence.Token) error { return blocked(ctx) })
				got, ok := f.results(t, 1)[0].(machine.Acquired)
				assert.True(t, ok, "the run must pass an Acquired")
				assert.ErrorIs(
					t,
					got.Err,
					context.DeadlineExceeded,
					"the Acquired must have the error of the ended call",
				)
			})
		})

		t.Run("passes ErrConfig for an Acquire without a guard", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Acquire{Fence: fencing, Token: 1}))
				got, ok := f.results(t, 1)[0].(machine.Acquired)
				assert.True(t, ok, "the run must pass an Acquired")
				expect.That(t, got.Err).ErrorIs(driver.ErrConfig, "the Acquired must have ErrConfig")
				expect.Equal(t, got.Token, machine.Token(1), "the Acquired must have the token of the Acquire")
			})
		})
	})
}
